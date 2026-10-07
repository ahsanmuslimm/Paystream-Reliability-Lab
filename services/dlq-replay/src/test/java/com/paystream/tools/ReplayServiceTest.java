package com.paystream.tools;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Replay accounting and mechanics with in-memory consumer/producer doubles:
 * raw bytes and keys flow to the header-named topic, offsets are committed,
 * and records without provenance are skipped (counted, never lost silently).
 */
class ReplayServiceTest {

    private static final String DLQ = "bank.transactions.v1.dlq";
    private static final TopicPartition DLQ_P0 = new TopicPartition(DLQ, 0);

    private MockConsumer<byte[], byte[]> consumer;
    private MockProducer<byte[], byte[]> producer;

    @BeforeEach
    void setUp() {
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.updateBeginningOffsets(Map.of(DLQ_P0, 0L));
        consumer.updateEndOffsets(Map.of(DLQ_P0, 0L));
        producer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
    }

    private ConsumerRecord<byte[], byte[]> dlqRecord(long offset, String target, String value) {
        ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>(DLQ, 0, offset, "key-1".getBytes(StandardCharsets.UTF_8),
                        value.getBytes(StandardCharsets.UTF_8));
        if (target != null) {
            record.headers().add(ReplayService.HEADER_ORIGINAL_TOPIC,
                    target.getBytes(StandardCharsets.UTF_8));
        }
        return record;
    }

    @Test
    void replaysRawBytesAndKeysToTheHeaderNamedTopicAndCommits() {
        consumer.schedulePollTask(() -> {
            consumer.rebalance(java.util.List.of(DLQ_P0));
            consumer.addRecord(dlqRecord(0L, "bank.transactions.v1", "raw-one"));
            consumer.addRecord(dlqRecord(1L, "bank.transactions.v1", "raw-two"));
        });
        ReplayService service = new ReplayService(consumer, producer, DLQ, null, false);

        ReplayService.Summary summary = service.replay(10);

        assertThat(summary.replayed()).isEqualTo(2);
        assertThat(summary.skipped()).isZero();
        assertThat(summary.failed()).isZero();
        assertThat(producer.history()).hasSize(2);
        assertThat(producer.history().get(0).topic()).isEqualTo("bank.transactions.v1");
        assertThat(new String(producer.history().get(0).value(), StandardCharsets.UTF_8)).isEqualTo("raw-one");
        assertThat(new String(producer.history().get(1).value(), StandardCharsets.UTF_8)).isEqualTo("raw-two");
        assertThat(consumer.committed(DLQ_P0).offset()).isEqualTo(2L);
    }

    @Test
    void targetOverrideWinsOverTheHeader() {
        consumer.schedulePollTask(() -> {
            consumer.rebalance(java.util.List.of(DLQ_P0));
            consumer.addRecord(dlqRecord(0L, "bank.transactions.v1", "raw"));
        });
        ReplayService service = new ReplayService(consumer, producer, DLQ,
                "bank.transactions.v1.retry", false);

        service.replay(10);

        assertThat(producer.history().get(0).topic()).isEqualTo("bank.transactions.v1.retry");
    }

    @Test
    void recordsWithoutProvenanceAreSkippedAndCounted() {
        consumer.schedulePollTask(() -> {
            consumer.rebalance(java.util.List.of(DLQ_P0));
            consumer.addRecord(dlqRecord(0L, null, "orphan"));
            consumer.addRecord(dlqRecord(1L, "bank.transactions.v1", "good"));
            });
        ReplayService service = new ReplayService(consumer, producer, DLQ, null, false);

        ReplayService.Summary summary = service.replay(10);

        assertThat(summary.replayed()).isEqualTo(1);
        assertThat(summary.skipped()).isEqualTo(1);
        assertThat(producer.history()).hasSize(1);
    }

    @Test
    void dryRunCountsWithoutProducingOrCommitting() {
        consumer.schedulePollTask(() -> {
            consumer.rebalance(java.util.List.of(DLQ_P0));
            consumer.addRecord(dlqRecord(0L, "bank.transactions.v1", "raw"));
        });
        ReplayService service = new ReplayService(consumer, producer, DLQ, null, true);

        ReplayService.Summary summary = service.replay(10);

        assertThat(summary.replayed()).isEqualTo(1);
        assertThat(producer.history()).isEmpty();
        assertThat(consumer.committed(DLQ_P0)).isNull();
    }

    @Test
    void stopsAtTheLimit() {
        consumer.schedulePollTask(() -> {
            consumer.rebalance(java.util.List.of(DLQ_P0));
            consumer.addRecord(dlqRecord(0L, "bank.transactions.v1", "one"));
            consumer.addRecord(dlqRecord(1L, "bank.transactions.v1", "two"));
            });
        ReplayService service = new ReplayService(consumer, producer, DLQ, null, false);

        ReplayService.Summary summary = service.replay(1);

        assertThat(summary.total()).isEqualTo(1);
        assertThat(summary.replayed()).isEqualTo(1);
        assertThat(producer.history()).hasSize(1);
    }
}
