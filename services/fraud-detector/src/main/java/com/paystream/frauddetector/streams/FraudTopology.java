package com.paystream.frauddetector.streams;

import bank.events.FraudAlert;
import bank.events.Transaction;
import com.paystream.common.AvroSerdes;
import com.paystream.common.Topics;
import java.math.BigDecimal;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.Topology;

/**
 * Stream topology for the fraud detector (Stage 2, WP2.3).
 *
 * Two rules over the same keyed transaction stream:
 * - amount threshold: strictly above the configured limit, HIGH severity
 *   beyond twice the limit, MEDIUM otherwise (Stage 1 scope);
 * - velocity window: {@code velocityLimit} transactions per account inside a
 *   trailing {@code velocityWindowSeconds} window (event time), firing once
 *   per burst (FR-05).
 *
 * Deserialization and processing failures are routed to the DLQ by the
 * handlers configured in application.yml (FR-07) - malformed records never
 * reach the rules.
 */
public final class FraudTopology {

    private FraudTopology() {
    }

    public static Topology fraudTopology(StreamsBuilder builder,
                                         BigDecimal amountThreshold,
                                         int velocityLimit,
                                         long velocityWindowSeconds,
                                         String schemaRegistryUrl) {
        Serde<String> keySerde = Serdes.String();
        Serde<Transaction> txnSerde = AvroSerdes.transaction(schemaRegistryUrl);
        Serde<FraudAlert> alertSerde = AvroSerdes.fraudAlert(schemaRegistryUrl);

        KStream<String, Transaction> transactions = builder.stream(
                Topics.TRANSACTIONS, Consumed.with(keySerde, txnSerde));

        transactions
                .filter((accountId, txn) -> FraudRules.amountRuleFires(txn, amountThreshold))
                .mapValues(txn -> FraudRules.toAmountAlert(txn, amountThreshold))
                .to(Topics.FRAUD_ALERTS, Produced.with(keySerde, alertSerde));

        builder.addStateStore(VelocityProcessor.storeBuilder());
        transactions
                .processValues(
                        () -> new VelocityProcessor(velocityWindowSeconds * 1000L, velocityLimit),
                        VelocityProcessor.STORE_NAME)
                .filter((accountId, alert) -> alert != null)
                .to(Topics.FRAUD_ALERTS, Produced.with(keySerde, alertSerde));

        return builder.build();
    }
}
