package com.paystream.frauddetector.streams.dlq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * FR-07 adapter behaviour: deserialization failures publish to the DLQ and
 * let the Streams thread continue; a failing DLQ publish stops the thread
 * instead of losing the record silently.
 */
class DlqDeserializationExceptionHandlerTest {

    private ErrorHandlerContext context;

    @BeforeEach
    void setUp() {
        context = mock(ErrorHandlerContext.class);
        when(context.topic()).thenReturn("bank.transactions.v1");
        when(context.partition()).thenReturn(1);
        when(context.offset()).thenReturn(99L);
    }

    @Test
    void publishesAndContinuesOnDeserializationFailure() {
        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        var handler = new DlqDeserializationExceptionHandler(producer);
        handler.configure(Map.of("bootstrap.servers", "dummy:9092",
                "dlq.topic", "bank.transactions.v1.dlq",
                "dlq.consumer.group", "fraud-detector"));

        var response = handler.handle(context,
                new ConsumerRecord<>("bank.transactions.v1", 1, 99L, "key".getBytes(StandardCharsets.UTF_8),
                        "not avro".getBytes(StandardCharsets.UTF_8)),
                new IllegalStateException("bad record"));

        assertThat(response).isEqualTo(DlqDeserializationExceptionHandler.DeserializationHandlerResponse.CONTINUE);
        assertThat(producer.history()).hasSize(1);
        assertThat(new String(producer.history().get(0).value(), StandardCharsets.UTF_8)).isEqualTo("not avro");
    }

    @Test
    void failsWhenTheDlqPublishFails() {
        var handler = new DlqDeserializationExceptionHandler(
                new org.apache.kafka.clients.producer.MockProducer<byte[], byte[]>(false, null, null) {
                    @Override
                    public synchronized java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata> send(
                            ProducerRecord<byte[], byte[]> record,
                            org.apache.kafka.clients.producer.Callback callback) {
                        return java.util.concurrent.CompletableFuture.failedFuture(
                                new RuntimeException("dlq broker down"));
                    }
                });
        handler.configure(Map.of("bootstrap.servers", "dummy:9092"));

        var response = handler.handle(context,
                new ConsumerRecord<>("bank.transactions.v1", 1, 99L, null, new byte[0]),
                new IllegalStateException("bad record"));

        assertThat(response).isEqualTo(DlqDeserializationExceptionHandler.DeserializationHandlerResponse.FAIL);
    }
}
