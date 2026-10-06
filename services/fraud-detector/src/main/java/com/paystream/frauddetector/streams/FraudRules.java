package com.paystream.frauddetector.streams;

import bank.events.FraudAlert;
import bank.events.Transaction;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Pure fraud rule logic (T1 unit-test target). The amount rule fires when a
 * transaction is strictly above the configured threshold; severity is HIGH
 * above twice the threshold, MEDIUM otherwise.
 */
public final class FraudRules {

    public static final String AMOUNT_RULE = "AMOUNT_THRESHOLD";

    private FraudRules() {
    }

    public static boolean amountRuleFires(Transaction txn, BigDecimal threshold) {
        return txn.getAmount().compareTo(threshold) > 0;
    }

    public static FraudAlert toAmountAlert(Transaction txn, BigDecimal threshold) {
        BigDecimal amount = txn.getAmount();
        bank.events.Severity severity = amount.compareTo(threshold.multiply(BigDecimal.valueOf(2))) > 0
                ? bank.events.Severity.HIGH : bank.events.Severity.MEDIUM;
        Map<String, String> details = new HashMap<>();
        details.put("amount", amount.toPlainString());
        details.put("threshold", threshold.toPlainString());
        details.put("channel", txn.getChannel().toString());

        return FraudAlert.newBuilder()
                .setAlertId(UUID.randomUUID())
                .setTxnId(txn.getTxnId())
                .setAccountId(txn.getAccountId())
                .setRuleName(AMOUNT_RULE)
                .setSeverity(severity)
                .setDetectedAt(Instant.now())
                .setDetails(details)
                .build();
    }
}
