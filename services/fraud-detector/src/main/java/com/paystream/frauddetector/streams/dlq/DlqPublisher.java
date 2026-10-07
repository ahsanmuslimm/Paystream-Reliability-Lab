package com.paystream.frauddetector.streams.dlq;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes poison records to the dead-letter topic with the exact header set
 * of Document 03 section 3 (ADR-0006). The DLQ value is the original raw bytes
 * so the record can be inspected and replayed unchanged.
 *
 * <p>Shared by both the Streams deserialization handler and the Streams
 * processing handler; the notifier consumer has its own recoverer in the
 * notifier module that emits the same header set.
 */
public final class DlqPublisher {

    public static final String HEADER_ORIGINAL_TOPIC = "dlq.original.topic";
    public static final String HEADER_ORIGINAL_PARTITION = "dlq.original.partition";
    public static final String HEADER_ORIGINAL_OFFSET = "dlq.original.offset";
    public static final String HEADER_ERROR_CLASS = "dlq.error.class";
    public static final String HEADER_ERROR_MESSAGE = "dlq.error.message";
    public static final String HEADER_FAILED_AT = "dlq.failed.at";
    public static final String HEADER_CONSUMER_GROUP = "dlq.consumer.group";
    public static final String HEADER_ATTEMPTS = "dlq.attempts";

    /** Error messages are truncated so a hostile payload cannot bloat the DLQ. */
    static final int MAX_ERROR_MESSAGE_LENGTH = 512;

    private static final Logger log = LoggerFactory.getLogger(DlqPublisher.class);
    private static final DateTimeFormatter ISO_UTC =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);

    private final Producer<byte[], byte[]> producer;
    private final String dlqTopic;
    private final String consumerGroup;

    public DlqPublisher(Producer<byte[], byte[]> producer, String dlqTopic, String consumerGroup) {
        this.producer = producer;
        this.dlqTopic = dlqTopic;
        this.consumerGroup = consumerGroup;
    }

    /** Result of a publish attempt; FAIL tells the caller to stop the Streams thread. */
    public enum PublishResult { PUBLISHED, FAILED }

    public PublishResult publish(String originalTopic, int partition, long offset,
                                 byte[] rawValue, String errorClass, String errorMessage) {
        return publish(originalTopic, partition, offset, null, rawValue, errorClass, errorMessage, 1);
    }

    public PublishResult publish(String originalTopic, int partition, long offset,
                                 byte[] rawValue, String errorClass, String errorMessage, int attempts) {
        return publish(originalTopic, partition, offset, null, rawValue, errorClass, errorMessage, attempts);
    }

    public PublishResult publish(String originalTopic, int partition, long offset, byte[] originalKey,
                                 byte[] rawValue, String errorClass, String errorMessage, int attempts) {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(dlqTopic, null, originalKey, rawValue);
        Headers headers = record.headers();
        headers.add(HEADER_ORIGINAL_TOPIC, originalTopic.getBytes(StandardCharsets.UTF_8));
        headers.add(HEADER_ORIGINAL_PARTITION, String.valueOf(partition).getBytes(StandardCharsets.UTF_8));
        headers.add(HEADER_ORIGINAL_OFFSET, String.valueOf(offset).getBytes(StandardCharsets.UTF_8));
        headers.add(HEADER_ERROR_CLASS, errorClass.getBytes(StandardCharsets.UTF_8));
        headers.add(HEADER_ERROR_MESSAGE, truncate(errorMessage).getBytes(StandardCharsets.UTF_8));
        headers.add(HEADER_FAILED_AT, ISO_UTC.format(Instant.now()).getBytes(StandardCharsets.UTF_8));
        headers.add(HEADER_CONSUMER_GROUP, consumerGroup.getBytes(StandardCharsets.UTF_8));
        headers.add(HEADER_ATTEMPTS, String.valueOf(attempts).getBytes(StandardCharsets.UTF_8));
        try {
            producer.send(record).get();
            log.warn("Record from {}:{}@{} moved to DLQ {} ({})",
                    originalTopic, partition, offset, dlqTopic, errorClass);
            return PublishResult.PUBLISHED;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return PublishResult.FAILED;
        } catch (Exception e) {
            log.error("Could not publish record from {}:{}@{} to DLQ {}: {}",
                    originalTopic, partition, offset, dlqTopic, e.toString());
            return PublishResult.FAILED;
        }
    }

    static String truncate(String message) {
        if (message == null) {
            return "";
        }
        String oneLine = message.replace('\n', ' ').replace('\r', ' ');
        return oneLine.length() <= MAX_ERROR_MESSAGE_LENGTH ? oneLine : oneLine.substring(0, MAX_ERROR_MESSAGE_LENGTH);
    }
}
