package com.paystream.notifier.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Entity
@Table(name = "fraud_alerts")
public class FraudAlertEntity {

    @Id
    @Column(name = "alert_id")
    private UUID alertId;

    @Column(name = "txn_id", nullable = false)
    private UUID txnId;

    @Column(name = "account_id", nullable = false)
    private UUID accountId;

    @Column(name = "rule_name", nullable = false, length = 64)
    private String ruleName;

    @Column(nullable = false, length = 8)
    private String severity;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private Map<String, String> details;

    @Column(name = "detected_at", nullable = false)
    private Instant detectedAt;

    @Column(name = "created_at", nullable = false, insertable = false, updatable = false)
    private Instant createdAt;

    protected FraudAlertEntity() {
    }

    public FraudAlertEntity(UUID alertId, UUID txnId, UUID accountId, String ruleName,
                            String severity, Map<String, String> details, Instant detectedAt) {
        this.alertId = alertId;
        this.txnId = txnId;
        this.accountId = accountId;
        this.ruleName = ruleName;
        this.severity = severity;
        this.details = details;
        this.detectedAt = detectedAt;
    }

    public UUID getAlertId() {
        return alertId;
    }

    public UUID getTxnId() {
        return txnId;
    }

    public UUID getAccountId() {
        return accountId;
    }

    public String getRuleName() {
        return ruleName;
    }

    public String getSeverity() {
        return severity;
    }

    public Map<String, String> getDetails() {
        return details;
    }

    public Instant getDetectedAt() {
        return detectedAt;
    }

    @Transient
    public boolean isHighSeverity() {
        return "HIGH".equals(severity);
    }
}
