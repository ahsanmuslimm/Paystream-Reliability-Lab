package com.paystream.frauddetector.config;

import com.paystream.frauddetector.streams.FraudTopology;
import java.math.BigDecimal;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    @Bean
    public Topology fraudTopology(StreamsBuilder builder,
                                  @Value("${paystream.fraud.amount-threshold:10000}") BigDecimal threshold,
                                  @Value("${paystream.schema-registry-url:http://schema-registry:8081}") String schemaRegistryUrl) {
        return FraudTopology.amountRuleTopology(builder, threshold, schemaRegistryUrl);
    }
}
