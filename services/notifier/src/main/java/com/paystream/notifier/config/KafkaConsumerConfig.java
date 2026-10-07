package com.paystream.notifier.config;

import bank.events.FraudAlert;
import com.paystream.common.Topics;
import com.paystream.notifier.error.DlqRecoverer;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Consumer configuration implementing the Document 03 section 5.3 baselines:
 * manual offsets, cooperative-sticky assignment, 45 s session timeout, 300 s
 * max poll interval.
 *
 * <p>WP2.3 hardening (FR-07): deserialization failures are non-retryable and
 * go straight to the DLQ via {@link DlqRecoverer}; other failures get two
 * backoff retries (1 s apart) before recovery. Poison records land in
 * {@code bank.fraud-alerts.v1.dlq} with the Document 03 header set and the
 * consumer keeps running.
 */
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, FraudAlert> fraudAlertConsumerFactory(KafkaProperties kafkaProperties) {
        var props = kafkaProperties.buildConsumerProperties(null);
        props.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Dedicated raw-byte producer for the DLQ: the DLQ value is the original
     * raw bytes (Document 03 section 3), not a re-serialized Avro object.
     * Inherits the bootstrap/security configuration from spring.kafka.
     */
    @Bean
    public KafkaOperations<String, byte[]> dlqKafkaTemplate(KafkaProperties kafkaProperties) {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties(null));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    @Bean
    public DlqRecoverer dlqRecoverer(KafkaOperations<String, byte[]> dlqKafkaTemplate) {
        return new DlqRecoverer(dlqKafkaTemplate, Topics.FRAUD_ALERTS_DLQ, Topics.GROUP_NOTIFIER);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, FraudAlert> fraudAlertContainerFactory(
            ConsumerFactory<String, FraudAlert> fraudAlertConsumerFactory,
            DlqRecoverer dlqRecoverer) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, FraudAlert>();
        factory.setConsumerFactory(fraudAlertConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(dlqRecoverer, new FixedBackOff(1_000L, 2));
        // poison is never fixed by retrying - recover immediately
        errorHandler.addNotRetryableExceptions(DeserializationException.class);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
