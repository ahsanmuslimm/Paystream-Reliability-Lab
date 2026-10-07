package com.paystream.notifier.error;

import static org.assertj.core.api.Assertions.assertThat;

import com.paystream.common.Topics;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.util.backoff.FixedBackOff;

/**
 * T6 precursor, wiring-level: the production error-handler chain against an
 * embedded broker - ErrorHandlingDeserializer (KIP-899-safe) ->
 * DefaultErrorHandler -> DlqRecoverer -> DLQ topic with the Document 03
 * headers, while the container keeps running and keeps delivering good
 * records. The cluster-level D6 drill re-proves this end to end.
 */
@EmbeddedKafka(partitions = 1, topics = {Topics.FRAUD_ALERTS, Topics.FRAUD_ALERTS_DLQ})
class NotifierDlqWiringTest {

    private static final String POISON_MARKER = "poison";
    private static final String GOOD_PAYLOAD = "good-payload";

    private final EmbeddedKafkaBroker broker; // injected by @EmbeddedKafka
    private ConcurrentMessageListenerContainer<String, byte[]> container;

    NotifierDlqWiringTest(EmbeddedKafkaBroker broker) {
        this.broker = broker;
    }

    /** Delegate deserializer that fails exactly like a corrupted payload would. */
    public static class PoisonAwareDeserializer implements org.apache.kafka.common.serialization.Deserializer<byte[]> {
        public PoisonAwareDeserializer() {
        }

        @Override
        public byte[] deserialize(String topic, byte[] data) {
            if (data != null && new String(data, StandardCharsets.UTF_8).contains(POISON_MARKER)) {
                throw new DeserializationException(
                        "simulated corrupt payload", data, false,
                        new IllegalArgumentException("not parseable"));
            }
            return data;
        }
    }

    @AfterEach
    void tearDown() {
        if (container != null) {
            container.stop();
        }
    }

    private Map<String, Object> consumerProps(String group) {
        Map<String, Object> props = KafkaTestUtils.consumerProps(group, "false", broker);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // MANUAL ack mode (prod parity)
        props.put("key.deserializer", ErrorHandlingDeserializer.class);
        props.put("value.deserializer", ErrorHandlingDeserializer.class);
        props.put("spring.deserializer.key.delegate.class", StringDeserializer.class.getName());
        props.put("spring.deserializer.value.delegate.class", PoisonAwareDeserializer.class.getName());
        return props;
    }

    private Producer<String, byte[]> rawProducer() {
        Map<String, Object> props = KafkaTestUtils.producerProps(broker);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", ByteArraySerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    @Test
    void poisonIsDeadLetteredWithDocument03HeadersAndGoodRecordsKeepFlowing() throws Exception {
        CountDownLatch delivered = new CountDownLatch(1);
        AtomicReference<String> deliveredKey = new AtomicReference<>();
        AtomicReference<String> deliveredValue = new AtomicReference<>();

        ContainerProperties containerProps = new ContainerProperties(Topics.FRAUD_ALERTS);
        // MANUAL matches the production KafkaConsumerConfig AckMode
        containerProps.setAckMode(ContainerProperties.AckMode.MANUAL);
        containerProps.setMessageListener(
                (org.springframework.kafka.listener.AcknowledgingMessageListener<String, byte[]>) (record, ack) -> {
                    // a real listener cannot work with a null value - surface it
                    if (record.value() == null) {
                        throw new IllegalStateException("null value (deserialization failed upstream)");
                    }
                    deliveredKey.set(record.key());
                    deliveredValue.set(new String(record.value(), StandardCharsets.UTF_8));
                    delivered.countDown();
                    ack.acknowledge();
                });
        container = new ConcurrentMessageListenerContainer<>(
                new DefaultKafkaConsumerFactory<>(consumerProps("t6-wiring")), containerProps);

        Map<String, Object> dlqSendProps = KafkaTestUtils.producerProps(broker);
        dlqSendProps.put("key.serializer", StringSerializer.class.getName());
        dlqSendProps.put("value.serializer", ByteArraySerializer.class.getName());
        DefaultKafkaProducerFactory<String, byte[]> dlqFactory =
                new DefaultKafkaProducerFactory<>(dlqSendProps);
        DlqRecoverer recoverer = new DlqRecoverer(
                new org.springframework.kafka.core.KafkaTemplate<>(dlqFactory),
                Topics.FRAUD_ALERTS_DLQ, Topics.GROUP_NOTIFIER);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(50L, 1));
        // poison is never fixed by retrying - recover immediately (same as prod config)
        errorHandler.addNotRetryableExceptions(DeserializationException.class);
        container.setCommonErrorHandler(errorHandler);
        container.start();

        Thread.sleep(500);
        try (Producer<String, byte[]> sender = rawProducer()) {
            sender.send(new ProducerRecord<>(Topics.FRAUD_ALERTS, "first-poison", POISON_MARKER.getBytes(StandardCharsets.UTF_8)));
            sender.send(new ProducerRecord<>(Topics.FRAUD_ALERTS, "after-poison", GOOD_PAYLOAD.getBytes(StandardCharsets.UTF_8)));
        }

        // the good record published after the poison one is still delivered
        assertThat(delivered.await(15, TimeUnit.SECONDS)).isTrue();
        assertThat(deliveredKey.get()).isEqualTo("after-poison");
        assertThat(deliveredValue.get()).isEqualTo(GOOD_PAYLOAD);
        assertThat(container.isRunning()).isTrue();

        // the DLQ receives exactly the poison record with the full header set
        Map<String, Object> readerProps = KafkaTestUtils.consumerProps("t6-dlq-reader", "true", broker);
        try (Consumer<String, byte[]> dlqConsumer = new KafkaConsumer<>(
                readerProps, new StringDeserializer(), new ByteArrayDeserializer())) {
            broker.consumeFromAnEmbeddedTopic(dlqConsumer, Topics.FRAUD_ALERTS_DLQ);
            ConsumerRecord<String, byte[]> dlqRecord =
                    KafkaTestUtils.getSingleRecord(dlqConsumer, Topics.FRAUD_ALERTS_DLQ, Duration.ofSeconds(15));

            assertThat(new String(dlqRecord.value(), StandardCharsets.UTF_8)).isEqualTo(POISON_MARKER);
            assertThat(header(dlqRecord, DlqRecoverer.HEADER_ORIGINAL_TOPIC)).isEqualTo(Topics.FRAUD_ALERTS);
            assertThat(header(dlqRecord, DlqRecoverer.HEADER_ERROR_CLASS))
                    .isEqualTo(DeserializationException.class.getName());
            assertThat(header(dlqRecord, DlqRecoverer.HEADER_CONSUMER_GROUP)).isEqualTo(Topics.GROUP_NOTIFIER);
            assertThat(header(dlqRecord, DlqRecoverer.HEADER_FAILED_AT)).endsWith("Z");
        }
    }

    private static String header(ConsumerRecord<String, byte[]> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
