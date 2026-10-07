package com.paystream.frauddetector.streams;

import bank.events.FraudAlert;
import bank.events.Transaction;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Velocity processor (WP2.3, FR-05): keeps a per-account list of event times
 * in a keyed state store and emits a VELOCITY_WINDOW alert on the
 * transaction that reaches the configured limit inside the trailing window.
 * Keying by account_id (ADR-0003) guarantees per-account ordering, which
 * this rule depends on.
 *
 * <p>Fixed-key by design (KIP-820): the rule only transforms values, the
 * account key flows through unchanged. Non-triggering transactions forward a
 * null value which the topology filters out, so the alerts topic only
 * receives real alerts.
 */
public class VelocityProcessor implements FixedKeyProcessor<String, Transaction, FraudAlert> {

    public static final String STORE_NAME = "account-txn-window";

    private static final Logger log = LoggerFactory.getLogger(VelocityProcessor.class);

    private final long windowMs;
    private final int limit;

    private FixedKeyProcessorContext<String, FraudAlert> context;
    private KeyValueStore<String, List<Long>> store;

    public VelocityProcessor(long windowMs, int limit) {
        this.windowMs = windowMs;
        this.limit = limit;
    }

    /** Store builder; added to the topology before the processor connects. */
    public static StoreBuilder<KeyValueStore<String, List<Long>>> storeBuilder() {
        return Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STORE_NAME),
                        Serdes.String(),
                        Serdes.ListSerde(java.util.ArrayList.class, Serdes.Long()))
                .withLoggingEnabled(Map.of("min.insync.replicas", "2"));
    }

    @Override
    public void init(FixedKeyProcessorContext<String, FraudAlert> context) {
        this.context = context;
        this.store = context.getStateStore(STORE_NAME);
    }

    @Override
    public void process(FixedKeyRecord<String, Transaction> record) {
        Transaction txn = record.value();
        String accountId = record.key();
        if (accountId == null || txn == null) {
            // malformed routing is the deserialization/DLQ handler's job; skip
            return;
        }

        long eventTimeMs = record.timestamp() >= 0
                ? record.timestamp()
                : Instant.now().toEpochMilli();

        List<Long> history = new ArrayList<>(VelocityRule.copy(store.get(accountId)));
        boolean triggered = VelocityRule.onTransaction(history, eventTimeMs, windowMs, limit);
        store.put(accountId, history);

        if (triggered) {
            log.info("Velocity rule fired for account {}: {} txns in {}s",
                    accountId, limit, windowMs / 1000);
            context.forward(record.withValue(velocityAlert(record, history.size())));
        } else {
            context.forward(record.withValue(null));
        }
    }

    private FraudAlert velocityAlert(FixedKeyRecord<String, Transaction> record, int burstSize) {
        Transaction txn = record.value();
        Map<String, String> details = new HashMap<>();
        details.put("burst_size", String.valueOf(burstSize));
        details.put("window_seconds", String.valueOf(windowMs / 1000));
        details.put("limit", String.valueOf(limit));
        details.put("amount", txn.getAmount().toPlainString());
        details.put("channel", txn.getChannel().toString());

        return FraudAlert.newBuilder()
                .setAlertId(java.util.UUID.randomUUID())
                .setTxnId(txn.getTxnId())
                .setAccountId(txn.getAccountId())
                .setRuleName(FraudRules.VELOCITY_RULE)
                .setSeverity(bank.events.Severity.MEDIUM)
                .setDetectedAt(Instant.now())
                .setDetails(details)
                .build();
    }
}
