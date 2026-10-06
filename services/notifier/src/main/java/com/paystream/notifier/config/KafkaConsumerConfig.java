package com.paystream.notifier.config;

import bank.events.FraudAlert;
import com.paystream.common.Topics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Consumer configuration implementing the Document 03 section 5.3 baselines:
 * manual offsets, cooperative-sticky assignment, 45 s session timeout, 300 s
 * max poll interval. Unexpected errors retry twice with a 1 s backoff and are
 * then logged and skipped - full DLQ routing arrives in Stage 2 (WP2.3).
 */
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, FraudAlert> fraudAlertConsumerFactory(KafkaProperties kafkaProperties) {
        var props = kafkaProperties.buildConsumerProperties(null);
        props.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, FraudAlert> fraudAlertContainerFactory(
            ConsumerFactory<String, FraudAlert> fraudAlertConsumerFactory) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, FraudAlert>();
        factory.setConsumerFactory(fraudAlertConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(1_000L, 2));
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
