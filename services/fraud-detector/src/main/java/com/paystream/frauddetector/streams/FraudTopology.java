package com.paystream.frauddetector.streams;

import bank.events.FraudAlert;
import bank.events.Transaction;
import com.paystream.common.AvroSerdes;
import com.paystream.common.Topics;
import java.math.BigDecimal;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.Topology;

/**
 * Stream topology for the fraud detector.
 *
 * MVP: amount threshold rule only (Stage 1 scope). The velocity window rule
 * and the retry/DLQ routing arrive in Stage 2 (WP2.3) - the store declared
 * below is the anchor the velocity window will use.
 */
public final class FraudTopology {

    public static final String ACCOUNT_TXN_COUNT_STORE = "account-txn-count";

    private FraudTopology() {
    }

    public static Topology amountRuleTopology(StreamsBuilder builder,
                                              BigDecimal threshold,
                                              String schemaRegistryUrl) {
        Serde<String> keySerde = Serdes.String();
        Serde<Transaction> txnSerde = AvroSerdes.transaction(schemaRegistryUrl);
        Serde<FraudAlert> alertSerde = AvroSerdes.fraudAlert(schemaRegistryUrl);

        KStream<String, Transaction> transactions = builder.stream(
                Topics.TRANSACTIONS, Consumed.with(keySerde, txnSerde));

        transactions
                .filter((accountId, txn) -> FraudRules.amountRuleFires(txn, threshold))
                .mapValues(txn -> FraudRules.toAmountAlert(txn, threshold))
                .to(Topics.FRAUD_ALERTS, Produced.with(keySerde, alertSerde));

        // anchor state store: the Stage 2 velocity rule counts transactions per
        // account inside a 60 s window using this store
        transactions
                .groupByKey()
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(ACCOUNT_TXN_COUNT_STORE)
                        .withKeySerde(keySerde)
                        .withValueSerde(Serdes.Long()));

        return builder.build();
    }
}
