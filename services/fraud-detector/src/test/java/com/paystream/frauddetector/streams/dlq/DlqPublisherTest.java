package com.paystream.frauddetector.streams.dlq;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * T6 precursor: DLQ records carry the exact Document 03 section 3 header set
 * and the original raw value, so the replay tooling can re-publish them
 * unchanged.
 */
class DlqPublisherTest {

    private MockProducer<byte[], byte[]> producer;
    private DlqPublisher publisher;

    @BeforeEach
    void setUp() {
        producer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        publisher = new DlqPublisher(producer, "bank.transactions.v1.dlq", "fraud-detector");
    }

    private static String header(ProducerRecord<byte[], byte[]> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    @Test
    void publishesRawValueWithTheDocument03HeaderSet() {
        byte[] raw = "{\"broken\": true}".getBytes(StandardCharsets.UTF_8);

        DlqPublisher.PublishResult result = publisher.publish(
                "bank.transactions.v1", 2, 4242L, raw,
                "org.apache.kafka.common.errors.SerializationException",
                "could not deserialize", 3);

        assertThat(result).isEqualTo(DlqPublisher.PublishResult.PUBLISHED);
        assertThat(producer.history()).hasSize(1);

        ProducerRecord<byte[], byte[]> record = producer.history().get(0);
        assertThat(record.topic()).isEqualTo("bank.transactions.v1.dlq");
        assertThat(record.value()).isEqualTo(raw);
        assertThat(header(record, DlqPublisher.HEADER_ORIGINAL_TOPIC)).isEqualTo("bank.transactions.v1");
        assertThat(header(record, DlqPublisher.HEADER_ORIGINAL_PARTITION)).isEqualTo("2");
        assertThat(header(record, DlqPublisher.HEADER_ORIGINAL_OFFSET)).isEqualTo("4242");
        assertThat(header(record, DlqPublisher.HEADER_ERROR_CLASS))
                .isEqualTo("org.apache.kafka.common.errors.SerializationException");
        assertThat(header(record, DlqPublisher.HEADER_ERROR_MESSAGE)).isEqualTo("could not deserialize");
        assertThat(header(record, DlqPublisher.HEADER_FAILED_AT)).endsWith("Z");
        assertThat(header(record, DlqPublisher.HEADER_CONSUMER_GROUP)).isEqualTo("fraud-detector");
        assertThat(header(record, DlqPublisher.HEADER_ATTEMPTS)).isEqualTo("3");
    }

    @Test
    void errorMessagesAreTruncatedAndFlattenedToOneLine() {
        String hostile = "line one\nline two\r" + "x".repeat(2_000);

        publisher.publish("t", 0, 0L, new byte[0], "SomeError", hostile, 1);

        ProducerRecord<byte[], byte[]> record = producer.history().get(0);
        String message = header(record, DlqPublisher.HEADER_ERROR_MESSAGE);
        assertThat(message).doesNotContain("\n").doesNotContain("\r");
        assertThat(message.length()).isLessThanOrEqualTo(DlqPublisher.MAX_ERROR_MESSAGE_LENGTH);
        assertThat(message).startsWith("line one line two");
    }

    @Test
    void reportsFailureWhenTheProducerCannotDeliver() {
        MockProducer<byte[], byte[]> failing = new MockProducer<>(false, null, null) {
            @Override
            public synchronized java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata> send(
                    ProducerRecord<byte[], byte[]> record,
                    org.apache.kafka.clients.producer.Callback callback) {
                return java.util.concurrent.CompletableFuture.failedFuture(
                        new RuntimeException("broker unreachable"));
            }
        };
        DlqPublisher failingPublisher = new DlqPublisher(failing, "dlq", "group");

        DlqPublisher.PublishResult result =
                failingPublisher.publish("t", 0, 0L, new byte[0], "E", "m");

        assertThat(result).isEqualTo(DlqPublisher.PublishResult.FAILED);
    }
}
