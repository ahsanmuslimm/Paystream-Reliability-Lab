package com.paystream.common;

import bank.events.FraudAlert;
import bank.events.Notification;
import bank.events.Transaction;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

/**
 * Avro Serde factory for the specific records generated from
 * kafka-config/schemas. All services use TopicNameStrategy subjects and
 * BACKWARD compatibility (ADR-0004).
 *
 * A {@code mock://scope} registry URL works without a running Schema Registry
 * and is used by unit tests and TopologyTestDriver harnesses.
 */
public final class AvroSerdes {

    private AvroSerdes() {
    }

    public static Serde<Transaction> transaction(String registryUrl) {
        return serdeFor(Transaction.class, registryUrl);
    }

    public static Serde<FraudAlert> fraudAlert(String registryUrl) {
        return serdeFor(FraudAlert.class, registryUrl);
    }

    public static Serde<Notification> notification(String registryUrl) {
        return serdeFor(Notification.class, registryUrl);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <T> Serde<T> serdeFor(Class<T> type, String registryUrl) {
        Map<String, Object> config = Map.of(
                "schema.registry.url", registryUrl,
                "specific.avro.reader", true);
        KafkaAvroSerializer serializer = new KafkaAvroSerializer();
        serializer.configure(config, false);
        KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
        deserializer.configure(config, false);
        return (Serde<T>) Serdes.serdeFrom(serializer, deserializer);
    }
}
