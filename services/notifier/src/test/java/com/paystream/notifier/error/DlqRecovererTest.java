package com.paystream.notifier.error;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.support.KafkaHeaders;

/**
 * FR-07: the notifier's dead-letter path publishes the original raw bytes
 * with the Document 03 header set (ADR-0006) and never blocks the consumer.
 */
class DlqRecovererTest {

    private static final String PAYLOAD = "raw-poison-bytes";

    @SuppressWarnings("unchecked")
    private final KafkaOperations<String, byte[]> template = mock(KafkaOperations.class);

    private DlqRecoverer recoverer;

    @BeforeEach
    void setUp() {
        recoverer = new DlqRecoverer(template, "bank.fraud-alerts.v1.dlq", "notifier");
    }

    private static String header(ProducerRecord<String, byte[]> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    @Test
    void deadLettersRawPayloadWithTheDocument03HeaderSet() {
        ConsumerRecord<String, byte[]> record =
                new ConsumerRecord<>("bank.fraud-alerts.v1", 0, 7L, "alert-1", PAYLOAD.getBytes(StandardCharsets.UTF_8));
        record.headers().add(KafkaHeaders.DELIVERY_ATTEMPT, java.nio.ByteBuffer.allocate(4).putInt(3).array());

        recoverer.accept(record, new IllegalStateException("boom"));

        ArgumentCaptor<ProducerRecord<String, byte[]>> captor =
                ArgumentCaptor.forClass((Class) ProducerRecord.class);
        verify(template).send(captor.capture());
        ProducerRecord<String, byte[]> sent = captor.getValue();

        assertThat(sent.topic()).isEqualTo("bank.fraud-alerts.v1.dlq");
        assertThat(sent.key()).isEqualTo("alert-1");
        assertThat(new String(sent.value(), StandardCharsets.UTF_8)).isEqualTo(PAYLOAD);
        assertThat(header(sent, DlqRecoverer.HEADER_ORIGINAL_TOPIC)).isEqualTo("bank.fraud-alerts.v1");
        assertThat(header(sent, DlqRecoverer.HEADER_ORIGINAL_PARTITION)).isEqualTo("0");
        assertThat(header(sent, DlqRecoverer.HEADER_ORIGINAL_OFFSET)).isEqualTo("7");
        assertThat(header(sent, DlqRecoverer.HEADER_ERROR_CLASS)).isEqualTo("java.lang.IllegalStateException");
        assertThat(header(sent, DlqRecoverer.HEADER_ERROR_MESSAGE)).isEqualTo("boom");
        assertThat(header(sent, DlqRecoverer.HEADER_FAILED_AT)).endsWith("Z");
        assertThat(header(sent, DlqRecoverer.HEADER_CONSUMER_GROUP)).isEqualTo("notifier");
        assertThat(header(sent, DlqRecoverer.HEADER_ATTEMPTS)).isEqualTo("3");
    }

    @Test
    void skipsRecordsWithoutARawPayloadInsteadOfBlockingTheConsumer() {
        ConsumerRecord<String, Object> record =
                new ConsumerRecord<>("bank.fraud-alerts.v1", 0, 7L, "alert-1", "already-parsed");

        recoverer.accept(record, new IllegalStateException("db down"));

        verify(template, never()).send(any(ProducerRecord.class));
    }
}
