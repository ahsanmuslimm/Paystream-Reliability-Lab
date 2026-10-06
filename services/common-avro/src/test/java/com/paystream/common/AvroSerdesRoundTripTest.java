package com.paystream.common;

import static org.assertj.core.api.Assertions.assertThat;

import bank.events.Channel;
import bank.events.FraudAlert;
import bank.events.Transaction;
import bank.events.TxnType;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.common.serialization.Serde;
import org.junit.jupiter.api.Test;

/**
 * T1: verifies the generated specific records round-trip through the
 * Confluent Avro serializer - in particular the decimal logical type mapping
 * to BigDecimal and the enum fields. Uses a mock:// registry so no server is
 * required.
 */
class AvroSerdesRoundTripTest {

    private Transaction sampleTransaction() {
        return Transaction.newBuilder()
                .setTxnId(UUID.randomUUID())
                .setAccountId(UUID.randomUUID())
                .setAmount(new BigDecimal("12345.67"))
                .setCurrency("USD")
                .setType(TxnType.DEBIT)
                .setChannel(Channel.ONLINE)
                .setMerchantId("MERCH-42")
                .setEventTime(Instant.now())
                .setMetadata(Map.of("source", "test"))
                .build();
    }

    private FraudAlert sampleAlert() {
        return FraudAlert.newBuilder()
                .setAlertId(UUID.randomUUID())
                .setTxnId(UUID.randomUUID())
                .setAccountId(UUID.randomUUID())
                .setRuleName("AMOUNT_THRESHOLD")
                .setSeverity(bank.events.Severity.HIGH)
                .setDetectedAt(Instant.now())
                .setDetails(Map.of("amount", "12345.67"))
                .build();
    }

    @Test
    void transactionRoundTripsWithDecimalAmount() {
        Serde<Transaction> serde = AvroSerdes.transaction("mock://roundtrip");
        Transaction original = sampleTransaction();

        byte[] bytes = serde.serializer().serialize("bank.transactions.v1", original);
        Transaction decoded = serde.deserializer().deserialize("bank.transactions.v1", bytes);

        assertThat(decoded.getTxnId()).isEqualTo(original.getTxnId());
        assertThat(decoded.getAmount()).isEqualByComparingTo(new BigDecimal("12345.67"));
        assertThat(decoded.getType()).isEqualTo(TxnType.DEBIT);
        assertThat(decoded.getChannel()).isEqualTo(Channel.ONLINE);
        assertThat(decoded.getMetadata()).containsEntry("source", "test");
    }

    @Test
    void fraudAlertRoundTrips() {
        Serde<FraudAlert> serde = AvroSerdes.fraudAlert("mock://roundtrip");
        FraudAlert original = sampleAlert();

        byte[] bytes = serde.serializer().serialize("bank.fraud-alerts.v1", original);
        FraudAlert decoded = serde.deserializer().deserialize("bank.fraud-alerts.v1", bytes);

        assertThat(decoded.getAlertId()).isEqualTo(original.getAlertId());
        assertThat(decoded.getSeverity()).isEqualTo(bank.events.Severity.HIGH);
        assertThat(decoded.getDetails()).containsEntry("amount", "12345.67");
    }

    @Test
    void accountPoolIsDeterministic() {
        AccountPool first = new AccountPool(200);
        AccountPool second = new AccountPool(200);

        assertThat(first.get(7).accountId()).isEqualTo(second.get(7).accountId());
        assertThat(first.get(7).customerId()).isEqualTo(second.get(7).customerId());
        assertThat(first.get(207).accountId()).isEqualTo(first.get(7).accountId());
    }
}
