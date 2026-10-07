package com.paystream.frauddetector.streams;

import static org.assertj.core.api.Assertions.assertThat;

import bank.events.Channel;
import bank.events.FraudAlert;
import bank.events.Transaction;
import bank.events.TxnType;
import com.paystream.common.AvroSerdes;
import com.paystream.common.Topics;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * T5: topology behaviour with TopologyTestDriver and a mock schema registry.
 * Covers both rules end to end: the amount threshold (Stage 1 scope) and the
 * Stage 2 velocity window, including its state store.
 */
class FraudTopologyTest {

    private static final BigDecimal THRESHOLD = new BigDecimal("10000.00");
    private static final int VELOCITY_LIMIT = 5;
    private static final long VELOCITY_WINDOW_S = 60;
    private static final String MOCK_REGISTRY = "mock://topology-test";
    private static final long BASE_MS = 1_700_000_000_000L;

    private static final UUID ACCOUNT_1 = UUID.fromString("00000000-0000-0000-0000-000000000001");
    private static final UUID ACCOUNT_2 = UUID.fromString("00000000-0000-0000-0000-000000000002");
    private static final UUID ACCOUNT_3 = UUID.fromString("00000000-0000-0000-0000-000000000003");
    private static final UUID ACCOUNT_4 = UUID.fromString("00000000-0000-0000-0000-000000000004");

    @TempDir
    Path stateDir;

    private TopologyTestDriver driver;
    private TestInputTopic<String, Transaction> input;
    private TestOutputTopic<String, FraudAlert> output;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        var topology = FraudTopology.fraudTopology(
                builder, THRESHOLD, VELOCITY_LIMIT, VELOCITY_WINDOW_S, MOCK_REGISTRY);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraud-topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());

        driver = new TopologyTestDriver(topology, props);

        Serde<Transaction> txnSerde = AvroSerdes.transaction(MOCK_REGISTRY);
        Serde<FraudAlert> alertSerde = AvroSerdes.fraudAlert(MOCK_REGISTRY);
        input = driver.createInputTopic(Topics.TRANSACTIONS,
                Serdes.String().serializer(), txnSerde.serializer());
        output = driver.createOutputTopic(Topics.FRAUD_ALERTS,
                Serdes.String().deserializer(), alertSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    private Transaction txn(UUID accountId, String amount) {
        return Transaction.newBuilder()
                .setTxnId(UUID.randomUUID())
                .setAccountId(accountId)
                .setAmount(new BigDecimal(amount))
                .setCurrency("USD")
                .setType(TxnType.DEBIT)
                .setChannel(Channel.POS)
                .setEventTime(Instant.now())
                .build();
    }

    private void pipeBurst(UUID accountId, int count, String amount, long startMs, long stepMs) {
        for (int i = 0; i < count; i++) {
            input.pipeInput(accountId.toString(), txn(accountId, amount), startMs + i * stepMs);
        }
    }

    // ---- amount rule (Stage 1 scope, unchanged behaviour) ----

    @Test
    void transactionAboveThresholdProducesAlert() {
        input.pipeInput(ACCOUNT_1.toString(), txn(ACCOUNT_1, "15000.00"), BASE_MS);

        List<FraudAlert> alerts = output.readValuesToList();
        assertThat(alerts).hasSize(1);
        assertThat(alerts.get(0).getRuleName()).isEqualTo("AMOUNT_THRESHOLD");
        assertThat(alerts.get(0).getSeverity()).isEqualTo(bank.events.Severity.MEDIUM);
        assertThat(alerts.get(0).getAccountId()).isEqualTo(ACCOUNT_1);
    }

    @Test
    void transactionsBelowThresholdProduceNoAlert() {
        input.pipeInput(ACCOUNT_1.toString(), txn(ACCOUNT_1, "9999.99"), BASE_MS);
        input.pipeInput(ACCOUNT_2.toString(), txn(ACCOUNT_2, "10000.00"), BASE_MS + 1);

        assertThat(output.readValuesToList()).isEmpty();
    }

    @Test
    void highValueTransactionEscalatesToHighSeverity() {
        input.pipeInput(ACCOUNT_3.toString(), txn(ACCOUNT_3, "50000.00"), BASE_MS);

        List<FraudAlert> alerts = output.readValuesToList();
        assertThat(alerts).hasSize(1);
        assertThat(alerts.get(0).getSeverity()).isEqualTo(bank.events.Severity.HIGH);
    }

    @Test
    void mixedTrafficProducesAlertsOnlyForOffenders() {
        input.pipeInput(ACCOUNT_1.toString(), txn(ACCOUNT_1, "500.00"), BASE_MS);
        input.pipeInput(ACCOUNT_2.toString(), txn(ACCOUNT_2, "12000.00"), BASE_MS + 1);
        input.pipeInput(ACCOUNT_3.toString(), txn(ACCOUNT_3, "10000.00"), BASE_MS + 2);
        input.pipeInput(ACCOUNT_4.toString(), txn(ACCOUNT_4, "99999.00"), BASE_MS + 3);

        List<FraudAlert> alerts = output.readValuesToList();
        assertThat(alerts).hasSize(2);
        assertThat(alerts).extracting(FraudAlert::getAccountId)
                .containsExactlyInAnyOrder(ACCOUNT_2, ACCOUNT_4);
    }

    // ---- velocity rule (Stage 2, WP2.3) ----

    @Test
    void burstBelowLimitProducesNoVelocityAlert() {
        pipeBurst(ACCOUNT_1, VELOCITY_LIMIT - 1, "50.00", BASE_MS, 100);

        assertThat(output.readValuesToList()).isEmpty();
    }

    @Test
    void burstAtLimitFiresExactlyOneVelocityAlert() {
        pipeBurst(ACCOUNT_1, VELOCITY_LIMIT + 2, "50.00", BASE_MS, 100);

        List<FraudAlert> alerts = output.readValuesToList();
        assertThat(alerts).hasSize(1);
        assertThat(alerts.get(0).getRuleName()).isEqualTo("VELOCITY_WINDOW");
        assertThat(alerts.get(0).getSeverity()).isEqualTo(bank.events.Severity.MEDIUM);
        assertThat(alerts.get(0).getDetails().get("limit")).isEqualTo("5");
    }

    @Test
    void transactionsSpreadBeyondTheWindowDoNotFire() {
        pipeBurst(ACCOUNT_1, VELOCITY_LIMIT + 1, "50.00", BASE_MS, 61_000);

        assertThat(output.readValuesToList()).isEmpty();
    }

    @Test
    void windowSlideAllowsANewBurstToFireAgain() {
        pipeBurst(ACCOUNT_1, VELOCITY_LIMIT, "50.00", BASE_MS, 100);          // fires
        pipeBurst(ACCOUNT_1, VELOCITY_LIMIT, "50.00", BASE_MS + 65_000, 100); // old window expired

        List<FraudAlert> alerts = output.readValuesToList();
        assertThat(alerts).hasSize(2);
        assertThat(alerts).extracting(FraudAlert::getRuleName)
                .containsOnly("VELOCITY_WINDOW");
    }

    @Test
    void velocityIsIndependentPerAccount() {
        pipeBurst(ACCOUNT_1, VELOCITY_LIMIT, "50.00", BASE_MS, 100);
        pipeBurst(ACCOUNT_2, VELOCITY_LIMIT - 1, "50.00", BASE_MS, 100);

        List<FraudAlert> alerts = output.readValuesToList();
        assertThat(alerts).hasSize(1);
        assertThat(alerts.get(0).getAccountId()).isEqualTo(ACCOUNT_1);
    }

    @Test
    void velocityStoreTracksWindowHistoryPerAccount() {
        pipeBurst(ACCOUNT_1, 2, "50.00", BASE_MS, 100);

        var store = driver.getKeyValueStore(VelocityProcessor.STORE_NAME);
        @SuppressWarnings("unchecked")
        List<Long> history = (List<Long>) store.get(ACCOUNT_1.toString());
        assertThat(history).hasSize(2);
    }
}
