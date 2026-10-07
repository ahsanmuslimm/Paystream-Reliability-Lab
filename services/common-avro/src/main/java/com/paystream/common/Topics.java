package com.paystream.common;

/**
 * Topic catalogue constants - mirrors kafka-config/topics.yaml (source of truth).
 * Keep in sync; validate-config.py fails CI when topics.yaml changes without
 * the supporting configuration (FR-02, FR-16).
 */
public final class Topics {

    public static final String TRANSACTIONS = "bank.transactions.v1";
    public static final String FRAUD_ALERTS = "bank.fraud-alerts.v1";
    public static final String NOTIFICATIONS = "bank.notifications.v1";
    public static final String TRANSACTIONS_RETRY = "bank.transactions.v1.retry";
    public static final String TRANSACTIONS_DLQ = "bank.transactions.v1.dlq";
    public static final String FRAUD_ALERTS_RETRY = "bank.fraud-alerts.v1.retry";
    public static final String FRAUD_ALERTS_DLQ = "bank.fraud-alerts.v1.dlq";
    public static final String ACCOUNTS_CDC = "pg.public.accounts";

    public static final String GROUP_FRAUD_DETECTOR = "fraud-detector";
    public static final String GROUP_NOTIFIER = "notifier";

    private Topics() {
    }
}
