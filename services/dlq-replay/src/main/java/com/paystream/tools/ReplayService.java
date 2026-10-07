package com.paystream.tools;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replay logic for dead-letter queues (WP2.3, ADR-0006): reads a DLQ, and for
 * each record re-publishes the original key/value to the topic recorded in
 * the {@code dlq.original.topic} header (or an explicit --target override),
 * then commits the DLQ offset. Records without the original-topic header are
 * counted as skipped, never silently dropped from the accounting.
 */
public class ReplayService {

    static final String HEADER_ORIGINAL_TOPIC = "dlq.original.topic";

    private static final Logger log = LoggerFactory.getLogger(ReplayService.class);

    /** Immutable result of one replay run. */
    public record Summary(long replayed, long skipped, long failed) {
        public long total() {
            return replayed + skipped + failed;
        }
    }

    private final Consumer<byte[], byte[]> consumer;
    private final Producer<byte[], byte[]> producer;
    private final String dlqTopic;
    private final String targetOverride;
    private final boolean dryRun;

    public ReplayService(Consumer<byte[], byte[]> consumer,
                         Producer<byte[], byte[]> producer,
                         String dlqTopic,
                         String targetOverride,
                         boolean dryRun) {
        this.consumer = consumer;
        this.producer = producer;
        this.dlqTopic = dlqTopic;
        this.targetOverride = targetOverride;
        this.dryRun = dryRun;
    }

    public Summary replay(long limit) {
        long replayed = 0;
        long skipped = 0;
        long failed = 0;

        consumer.subscribe(List.of(dlqTopic));
        try {
            while (replayed + skipped + failed < limit) {
                var records = consumer.poll(Duration.ofSeconds(5));
                if (records.isEmpty()) {
                    break; // drained
                }
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    if (replayed + skipped + failed >= limit) {
                        break;
                    }
                    String target = targetOverride != null ? targetOverride : originalTopic(record);
                    if (target == null) {
                        log.warn("Skipping {}:{}@{} - no {} header and no --target override",
                                record.topic(), record.partition(), record.offset(), HEADER_ORIGINAL_TOPIC);
                        skipped++;
                    } else if (dryRun) {
                        replayed++;
                    } else {
                        try {
                            producer.send(new ProducerRecord<>(target, null,
                                    record.key(), record.value())).get();
                            consumer.commitSync(Map.of(
                                    new org.apache.kafka.common.TopicPartition(dlqTopic, record.partition()),
                                    new org.apache.kafka.clients.consumer.OffsetAndMetadata(record.offset() + 1)));
                            replayed++;
                        } catch (Exception e) {
                            log.error("Replay of {}:{}@{} to {} failed: {}",
                                    record.topic(), record.partition(), record.offset(), target, e.toString());
                            failed++;
                        }
                    }
                }
            }
        } finally {
            producer.flush();
        }
        return new Summary(replayed, skipped, failed);
    }

    private static String originalTopic(ConsumerRecord<byte[], byte[]> record) {
        Header header = record.headers().lastHeader(HEADER_ORIGINAL_TOPIC);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
