package com.paystream.notifier.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bank.events.FraudAlert;
import bank.events.Severity;
import com.paystream.common.AccountPool;
import com.paystream.notifier.repository.AccountRepository;
import com.paystream.notifier.repository.FraudAlertRepository;
import com.paystream.notifier.repository.NotificationRepository;
import com.paystream.notifier.repository.ProcessedEventRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;

/**
 * T1: idempotency branch logic (90%+ coverage target applies to this class).
 */
class AlertIngestionServiceTest {

    private final ProcessedEventRepository processedEvents = mock(ProcessedEventRepository.class);
    private final AccountRepository accounts = mock(AccountRepository.class);
    private final FraudAlertRepository fraudAlerts = mock(FraudAlertRepository.class);
    private final NotificationRepository notifications = mock(NotificationRepository.class);

    private AlertIngestionService service;
    private AccountPool pool;

    @BeforeEach
    void setUp() {
        service = new AlertIngestionService(processedEvents, accounts, fraudAlerts, notifications);
        pool = new AccountPool(200);
    }

    private FraudAlert alert(UUID alertId, UUID accountId) {
        return FraudAlert.newBuilder()
                .setAlertId(alertId)
                .setTxnId(UUID.randomUUID())
                .setAccountId(accountId)
                .setRuleName("AMOUNT_THRESHOLD")
                .setSeverity(Severity.MEDIUM)
                .setDetectedAt(Instant.now())
                .setDetails(Map.of("amount", "15000.00", "threshold", "10000.00"))
                .build();
    }

    @Test
    void freshAlertIsIngestedWithBusinessWrites() {
        UUID alertId = UUID.randomUUID();
        var account = pool.get(3);
        when(accounts.findById(account.accountId())).thenReturn(Optional.of(
                new com.paystream.notifier.domain.AccountEntity(
                        account.accountId(), account.customerId(), new BigDecimal("1000.00"), "ACTIVE")));

        var result = service.ingest(alert(alertId, account.accountId()),
                "bank.fraud-alerts.v1", 0, 0);

        assertThat(result.outcome()).isEqualTo(AlertIngestionService.Outcome.INGESTED);
        assertThat(result.customerId()).isEqualTo(account.customerId());
        assertThat(result.alertId()).isEqualTo(alertId);
        verify(fraudAlerts).save(any());
        verify(notifications).save(any());
    }

    @Test
    void duplicateAlertIsSkippedWithoutBusinessWrites() {
        UUID alertId = UUID.randomUUID();
        when(processedEvents.saveAndFlush(any()))
                .thenThrow(new DataIntegrityViolationException("duplicate key"));

        var result = service.ingest(alert(alertId, pool.get(5).accountId()),
                "bank.fraud-alerts.v1", 0, 0);

        assertThat(result.outcome()).isEqualTo(AlertIngestionService.Outcome.DUPLICATE);
        verify(fraudAlerts, never()).save(any());
        verify(notifications, never()).save(any());
    }

    @Test
    void unknownAccountFailsLoudly() {
        UUID alertId = UUID.randomUUID();
        when(accounts.findById(any())).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.ingest(alert(alertId, UUID.randomUUID()),
                "bank.fraud-alerts.v1", 0, 0))
                .isInstanceOf(IllegalStateException.class);
        verify(notifications, never()).save(any());
    }

    @Test
    void channelMappingFollowsSeverity() {
        assertThat(AlertIngestionService.channelFor("HIGH")).isEqualTo("SMS");
        assertThat(AlertIngestionService.channelFor("MEDIUM")).isEqualTo("PUSH");
        assertThat(AlertIngestionService.channelFor("LOW")).isEqualTo("EMAIL");
    }
}
