package com.paystream.notifier.error;

import com.paystream.common.Topics;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.support.KafkaHeaders;

/**
 * Dead-letter recoverer for the notifier consumer (WP2.3, FR-07).
 *
 * Poison records (deserialization failures arrive here as raw byte payloads)
 * are published to {@code bank.fraud-alerts.v1.dlq} with the Document 03
 * section 3 header set - the same header names the fraud-detector Streams
 * handlers use (ADR-0006) - so one replay tool covers both DLQs.
 *
 * A record that fails for a non-poison reason (e.g. the database was down
 * through all backoff attempts) has no raw bytes to preserve: its value was
 * already deserialized. Rather than silently losing it, the recoverer logs
 * at ERROR and skips; the lag and error alerts are the safety net for that
 * path. This trade-off is recorded in ADR-0006.
 */
public class DlqRecoverer implements ConsumerRecordRecoverer {

    // same header set as fraud-detector's DlqPublisher (Document 03 section 3)
    public static final String HEADER_ORIGINAL_TOPIC = "dlq.original.topic";
    public static final String HEADER_ORIGINAL_PARTITION = "dlq.original.partition";
    public static final String HEADER_ORIGINAL_OFFSET = "dlq.original.offset";
    public static final String HEADER_ERROR_CLASS = "dlq.error.class";
    public static final String HEADER_ERROR_MESSAGE = "dlq.error.message";
    public static final String HEADER_FAILED_AT = "dlq.failed.at";
    public static final String HEADER_CONSUMER_GROUP = "dlq.consumer.group";
    public static final String HEADER_ATTEMPTS = "dlq.attempts";

    static final int MAX_ERROR_MESSAGE_LENGTH = 512;

    private static final Logger log = LoggerFactory.getLogger(DlqRecoverer.class);
    private static final DateTimeFormatter ISO_UTC =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);

    private final KafkaOperations<String, byte[]> template;
    private final String dlqTopic;
    private final String consumerGroup;

    public DlqRecoverer(KafkaOperations<String, byte[]> template, String dlqTopic, String consumerGroup) {
        this.template = template;
        this.dlqTopic = dlqTopic;
        this.consumerGroup = consumerGroup;
    }

    @Override
    public void accept(ConsumerRecord<?, ?> record, Exception exception) {
        if (!(record.value() instanceof byte[] rawValue)) {
            log.error("Record {}:{}@{} failed after all retries but has no raw payload to "
                            + "dead-letter (error: {}). Skipping; the failure alert covers this path.",
                    record.topic(), record.partition(), record.offset(), exception.toString());
            return;
        }

        ProducerRecord<String, byte[]> dlqRecord =
                new ProducerRecord<>(dlqTopic, null, record.key() instanceof String key ? key : null, rawValue);
        Headers headers = dlqRecord.headers();
        headers.add(HEADER_ORIGINAL_TOPIC, bytes(record.topic()));
        headers.add(HEADER_ORIGINAL_PARTITION, bytes(String.valueOf(record.partition())));
        headers.add(HEADER_ORIGINAL_OFFSET, bytes(String.valueOf(record.offset())));
        headers.add(HEADER_ERROR_CLASS, bytes(exception.getClass().getName()));
        headers.add(HEADER_ERROR_MESSAGE, bytes(truncate(exception.getMessage())));
        headers.add(HEADER_FAILED_AT, bytes(ISO_UTC.format(Instant.now())));
        headers.add(HEADER_CONSUMER_GROUP, bytes(consumerGroup));
        headers.add(HEADER_ATTEMPTS, bytes(String.valueOf(deliveryAttempts(record))));

        template.send(dlqRecord);
        log.warn("Record {}:{}@{} moved to {} ({})",
                record.topic(), record.partition(), record.offset(), dlqTopic, exception.toString());
    }

    private int deliveryAttempts(ConsumerRecord<?, ?> record) {
        Header attempts = record.headers().lastHeader(KafkaHeaders.DELIVERY_ATTEMPT);
        if (attempts != null && attempts.value() != null && attempts.value().length == 4) {
            return java.nio.ByteBuffer.wrap(attempts.value()).getInt();
        }
        return 1;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    static String truncate(String message) {
        if (message == null) {
            return "";
        }
        String oneLine = message.replace('\n', ' ').replace('\r', ' ');
        return oneLine.length() <= MAX_ERROR_MESSAGE_LENGTH ? oneLine : oneLine.substring(0, MAX_ERROR_MESSAGE_LENGTH);
    }
}
