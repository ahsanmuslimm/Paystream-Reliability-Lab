package com.paystream.notifier.service;

import bank.events.Notification;
import bank.events.NotifyChannel;
import com.paystream.common.Topics;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Publishes the downstream Notification event after the database transaction
 * has committed. Publishing is best-effort at this stage: a lost publish
 * leaves the notification row in place and is logged - the drill programme
 * (Stage 2) turns this into a measured, discussed behaviour.
 */
@Service
public class NotificationPublisher {

    private static final Logger log = LoggerFactory.getLogger(NotificationPublisher.class);

    private final KafkaTemplate<String, Notification> template;
    private final String topic;

    public NotificationPublisher(KafkaTemplate<String, Notification> template,
                                 @Value("${paystream.topics.notifications}") String topic) {
        this.template = template;
        this.topic = topic;
    }

    public void publish(UUID customerId, UUID alertId, String severity) {
        Notification notification = Notification.newBuilder()
                .setNotificationId(UUID.randomUUID())
                .setCustomerId(customerId)
                .setAlertId(alertId)
                .setChannel(channelFor(severity))
                .setCreatedAt(Instant.now())
                .build();
        template.send(topic, customerId.toString(), notification)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Notification publish failed for alert {}: {}", alertId, ex.getMessage());
                    }
                });
    }

    static NotifyChannel channelFor(String severity) {
        return switch (AlertIngestionService.channelFor(severity)) {
            case "SMS" -> NotifyChannel.SMS;
            case "PUSH" -> NotifyChannel.PUSH;
            default -> NotifyChannel.EMAIL;
        };
    }
}
