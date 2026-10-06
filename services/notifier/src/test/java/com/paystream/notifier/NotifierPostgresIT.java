package com.paystream.notifier;

import static org.assertj.core.api.Assertions.assertThat;

import bank.events.FraudAlert;
import bank.events.Severity;
import com.paystream.common.AccountPool;
import com.paystream.notifier.repository.FraudAlertRepository;
import com.paystream.notifier.repository.NotificationRepository;
import com.paystream.notifier.repository.ProcessedEventRepository;
import com.paystream.notifier.service.AlertIngestionService;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * T3/T6 evidence: replaying the same alert ten times yields exactly one
 * notification row (FR-06 acceptance). Requires Docker; runs in CI and on
 * machines with a container runtime.
 */
@SpringBootTest(properties = {
        "spring.kafka.listener.auto-startup=false"
})
@Testcontainers(disabledWithoutDocker = true)
class NotifierPostgresIT {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @MockitoBean
    com.paystream.notifier.service.NotificationPublisher publisher;
    @Autowired
    AlertIngestionService ingestion;

    @Autowired
    AccountPool accountPool;

    @Autowired
    NotificationRepository notifications;

    @Autowired
    FraudAlertRepository fraudAlerts;

    @Autowired
    ProcessedEventRepository processedEvents;

    private FraudAlert alert(UUID alertId, UUID accountId) {
        return FraudAlert.newBuilder()
                .setAlertId(alertId)
                .setTxnId(UUID.randomUUID())
                .setAccountId(accountId)
                .setRuleName("AMOUNT_THRESHOLD")
                .setSeverity(Severity.HIGH)
                .setDetectedAt(Instant.now())
                .setDetails(Map.of("amount", "25000.00", "threshold", "10000.00"))
                .build();
    }

    @Test
    void replayingSameAlertTenTimesProducesExactlyOneNotification() {
        AccountPool.Account account = accountPool.get(11);
        UUID alertId = UUID.randomUUID();
        var theAlert = alert(alertId, account.accountId());

        int ingested = 0;
        for (int i = 0; i < 10; i++) {
            var result = ingestion.ingest(theAlert, "bank.fraud-alerts.v1", 0, i);
            if (result.ingested()) {
                ingested++;
            }
        }

        assertThat(ingested).isEqualTo(1);
        assertThat(notifications.countByAlertId(alertId)).isEqualTo(1);
        assertThat(fraudAlerts.count()).isEqualTo(1);
        assertThat(processedEvents.count()).isEqualTo(1);
    }

    @Test
    void distinctAlertsEachProduceTheirOwnNotification() {
        AccountPool.Account account = accountPool.get(12);
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();

        assertThat(ingestion.ingest(alert(first, account.accountId()), "bank.fraud-alerts.v1", 1, 0).ingested()).isTrue();
        assertThat(ingestion.ingest(alert(second, account.accountId()), "bank.fraud-alerts.v1", 1, 1).ingested()).isTrue();

        assertThat(notifications.count()).isEqualTo(2);
    }
}
