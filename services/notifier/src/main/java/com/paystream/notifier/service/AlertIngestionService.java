package com.paystream.notifier.service;

import bank.events.FraudAlert;
import com.paystream.common.Topics;
import com.paystream.notifier.domain.AccountEntity;
import com.paystream.notifier.domain.FraudAlertEntity;
import com.paystream.notifier.domain.NotificationEntity;
import com.paystream.notifier.domain.ProcessedEventEntity;
import com.paystream.notifier.repository.AccountRepository;
import com.paystream.notifier.repository.FraudAlertRepository;
import com.paystream.notifier.repository.NotificationRepository;
import com.paystream.notifier.repository.ProcessedEventRepository;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Idempotent ingestion of fraud alerts (FR-06).
 *
 * Implements the Document 03 pattern: in one database transaction insert the
 * processed_events marker; if the primary key (consumer_group, event_id)
 * conflicts the record is a duplicate and is skipped; otherwise perform the
 * business writes and commit. The Kafka offset is committed only after this
 * method returns, so a crash between commit and offset commit replays the
 * record - and the replay is collapsed by the marker.
 */
@Service
public class AlertIngestionService {

    public enum Outcome { INGESTED, DUPLICATE }

    public record IngestResult(Outcome outcome, UUID customerId, UUID alertId) {
        public boolean ingested() {
            return outcome == Outcome.INGESTED;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(AlertIngestionService.class);

    private final ProcessedEventRepository processedEvents;
    private final AccountRepository accounts;
    private final FraudAlertRepository fraudAlerts;
    private final NotificationRepository notifications;

    public AlertIngestionService(ProcessedEventRepository processedEvents,
                                 AccountRepository accounts,
                                 FraudAlertRepository fraudAlerts,
                                 NotificationRepository notifications) {
        this.processedEvents = processedEvents;
        this.accounts = accounts;
        this.fraudAlerts = fraudAlerts;
        this.notifications = notifications;
    }

    @Transactional
    public IngestResult ingest(FraudAlert alert, String topic, int partition, long offset) {
        UUID alertId = alert.getAlertId();
        UUID accountId = alert.getAccountId();

        try {
            processedEvents.saveAndFlush(new ProcessedEventEntity(
                    Topics.GROUP_NOTIFIER, alertId, topic, partition, offset));
        } catch (DataIntegrityViolationException e) {
            log.debug("Duplicate alert {} skipped", alertId);
            return new IngestResult(Outcome.DUPLICATE, null, alertId);
        }

        AccountEntity account = accounts.findById(accountId)
                .orElseThrow(() -> new IllegalStateException(
                        "Unknown account %s for alert %s".formatted(accountId, alertId)));

        fraudAlerts.save(new FraudAlertEntity(
                alertId,
                alert.getTxnId(),
                accountId,
                alert.getRuleName(),
                alert.getSeverity().name(),
                alert.getDetails() == null ? null : Map.copyOf(alert.getDetails()),
                alert.getDetectedAt()));

        NotificationEntity notification = new NotificationEntity(
                UUID.randomUUID(), alertId, account.getCustomerId(), channelFor(alert.getSeverity().name()));
        notifications.save(notification);

        log.info("Alert {} ingested (rule={}, severity={})", alertId, alert.getRuleName(), alert.getSeverity());
        return new IngestResult(Outcome.INGESTED, account.getCustomerId(), alertId);
    }

    static String channelFor(String severity) {
        return switch (severity) {
            case "HIGH" -> "SMS";
            case "MEDIUM" -> "PUSH";
            default -> "EMAIL";
        };
    }
}
