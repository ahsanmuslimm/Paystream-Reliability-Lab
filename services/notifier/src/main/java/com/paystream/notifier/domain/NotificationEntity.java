package com.paystream.notifier.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "notifications")
public class NotificationEntity {

    @Id
    @Column(name = "notification_id")
    private UUID notificationId;

    @Column(name = "alert_id", nullable = false, unique = true)
    private UUID alertId;

    @Column(name = "customer_id", nullable = false)
    private UUID customerId;

    @Column(nullable = false, length = 8)
    private String channel;

    @Column(nullable = false, length = 12)
    private String status;

    @Column(name = "created_at", nullable = false, insertable = false, updatable = false)
    private Instant createdAt;

    protected NotificationEntity() {
    }

    public NotificationEntity(UUID notificationId, UUID alertId, UUID customerId, String channel) {
        this.notificationId = notificationId;
        this.alertId = alertId;
        this.customerId = customerId;
        this.channel = channel;
        this.status = "SENT";
    }

    public UUID getNotificationId() {
        return notificationId;
    }

    public UUID getAlertId() {
        return alertId;
    }

    public UUID getCustomerId() {
        return customerId;
    }

    public String getChannel() {
        return channel;
    }

    public String getStatus() {
        return status;
    }
}
