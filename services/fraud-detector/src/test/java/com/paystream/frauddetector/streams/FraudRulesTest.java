package com.paystream.frauddetector.streams;

import static org.assertj.core.api.Assertions.assertThat;

import bank.events.Channel;
import bank.events.Transaction;
import bank.events.TxnType;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * T1: amount rule boundaries. The rule fires strictly above the threshold;
 * severity escalates to HIGH above twice the threshold.
 */
class FraudRulesTest {

    private static final BigDecimal THRESHOLD = new BigDecimal("10000.00");

    private Transaction txnOf(String amount) {
        return Transaction.newBuilder()
                .setTxnId(UUID.randomUUID())
                .setAccountId(UUID.randomUUID())
                .setAmount(new BigDecimal(amount))
                .setCurrency("USD")
                .setType(TxnType.DEBIT)
                .setChannel(Channel.ATM)
                .setEventTime(Instant.now())
                .build();
    }

    @Test
    void belowThresholdDoesNotFire() {
        assertThat(FraudRules.amountRuleFires(txnOf("9999.99"), THRESHOLD)).isFalse();
    }

    @Test
    void exactlyAtThresholdDoesNotFire() {
        assertThat(FraudRules.amountRuleFires(txnOf("10000.00"), THRESHOLD)).isFalse();
    }

    @Test
    void justAboveThresholdFiresWithMediumSeverity() {
        Transaction txn = txnOf("10000.01");
        assertThat(FraudRules.amountRuleFires(txn, THRESHOLD)).isTrue();
        assertThat(FraudRules.toAmountAlert(txn, THRESHOLD).getSeverity())
                .isEqualTo(bank.events.Severity.MEDIUM);
    }

    @Test
    void doubleThresholdBoundaryStaysMedium() {
        assertThat(FraudRules.toAmountAlert(txnOf("20000.00"), THRESHOLD).getSeverity())
                .isEqualTo(bank.events.Severity.MEDIUM);
    }

    @Test
    void aboveDoubleThresholdEscalatesToHigh() {
        assertThat(FraudRules.toAmountAlert(txnOf("20000.01"), THRESHOLD).getSeverity())
                .isEqualTo(bank.events.Severity.HIGH);
        assertThat(FraudRules.toAmountAlert(txnOf("25000.00"), THRESHOLD).getSeverity())
                .isEqualTo(bank.events.Severity.HIGH);
    }

    @Test
    void alertCarriesRuleNameAndSourceTransaction() {
        Transaction txn = txnOf("15000.00");
        var alert = FraudRules.toAmountAlert(txn, THRESHOLD);

        assertThat(alert.getRuleName()).isEqualTo("AMOUNT_THRESHOLD");
        assertThat(alert.getTxnId()).isEqualTo(txn.getTxnId());
        assertThat(alert.getAccountId()).isEqualTo(txn.getAccountId());
        assertThat(alert.getDetails())
                .containsEntry("amount", "15000.00")
                .containsEntry("threshold", "10000.00");
        assertThat(alert.getAlertId()).isNotNull();
        assertThat(alert.getDetectedAt()).isAfter(Instant.EPOCH);
    }
}
