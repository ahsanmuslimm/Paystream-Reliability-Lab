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
import org.apache.kafka.streams.TopologyTestDriver;import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * T5 precursor: topology behaviour with TopologyTestDriver and a mock schema
 * registry. Covers the amount-rule path end to end plus the per-account
 * counting store that the Stage 2 velocity rule will consume.
 */
class FraudTopologyTest {

    private static final BigDecimal THRESHOLD = new BigDecimal("10000.00");
    private static final String MOCK_REGISTRY = "mock://topology-test";

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
        var topology = FraudTopology.amountRuleTopology(builder, THRESHOLD, MOCK_REGISTRY);

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

    @Test
    void transactionAboveThresholdProducesAlert() {
        input.pipeInput(ACCOUNT_1.toString(), txn(ACCOUNT_1, "15000.00"));

        List<FraudAlert> alerts = output.readValuesToList();
        assertThat(alerts).hasSize(1);
        assertThat(alerts.get(0).getRuleName()).isEqualTo("AMOUNT_THRESHOLD");
        assertThat(alerts.get(0).getSeverity()).isEqualTo(bank.events.Severity.MEDIUM);
        assertThat(alerts.get(0).getAccountId()).isEqualTo(ACCOUNT_1);
    }

    @Test
    void transactionsBelowThresholdProduceNoAlert() {
        input.pipeInput(ACCOUNT_1.toString(), txn(ACCOUNT_1, "9999.99"));
        input.pipeInput(ACCOUNT_2.toString(), txn(ACCOUNT_2, "10000.00"));

        assertThat(output.readValuesToList()).isEmpty();
    }

    @Test
    void highValueTransactionEscalatesToHighSeverity() {
        input.pipeInput(ACCOUNT_3.toString(), txn(ACCOUNT_3, "50000.00"));

        List<FraudAlert> alerts = output.readValuesToList();
        assertThat(alerts).hasSize(1);
        assertThat(alerts.get(0).getSeverity()).isEqualTo(bank.events.Severity.HIGH);
    }

    @Test
    void mixedTrafficProducesAlertsOnlyForOffenders() {
        input.pipeInput(ACCOUNT_1.toString(), txn(ACCOUNT_1, "500.00"));
        input.pipeInput(ACCOUNT_2.toString(), txn(ACCOUNT_2, "12000.00"));
        input.pipeInput(ACCOUNT_3.toString(), txn(ACCOUNT_3, "10000.00"));
        input.pipeInput(ACCOUNT_4.toString(), txn(ACCOUNT_4, "99999.00"));

        List<FraudAlert> alerts = output.readValuesToList();
        assertThat(alerts).hasSize(2);
        assertThat(alerts).extracting(FraudAlert::getAccountId)
                .containsExactlyInAnyOrder(ACCOUNT_2, ACCOUNT_4);
    }

    @Test
    void countStoreTracksAllTransactionsPerAccount() {
        input.pipeInput(ACCOUNT_1.toString(), txn(ACCOUNT_1, "500.00"));
        input.pipeInput(ACCOUNT_1.toString(), txn(ACCOUNT_1, "700.00"));
        input.pipeInput(ACCOUNT_2.toString(), txn(ACCOUNT_2, "800.00"));

        var store = driver.getKeyValueStore("account-txn-count");
        assertThat(store.get(ACCOUNT_1.toString())).isEqualTo(2L);
        assertThat(store.get(ACCOUNT_2.toString())).isEqualTo(1L);
    }
}
