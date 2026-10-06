package com.paystream.txnproducer.generator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import bank.events.Transaction;
import com.paystream.common.AccountPool;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * T1: generator determinism and workload shape. ~10% high-value traffic keeps
 * the end-to-end flow alive; keys are account UUIDs from the shared pool so
 * per-account ordering holds.
 */
class SyntheticTransactionGeneratorTest {

    private static final String TOPIC = "bank.transactions.v1";

    private final KafkaTemplate<String, Transaction> template = mock(KafkaTemplate.class);
    private SyntheticTransactionGenerator generator;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        when(template.send(anyString(), anyString(), any(Transaction.class)))
                .thenAnswer(inv -> CompletableFuture.completedFuture(null));
        generator = new SyntheticTransactionGenerator(
                template, new AccountPool(200), TOPIC, new SimpleMeterRegistry(),
                new SplittableRandom(42));
    }

    @Test
    void generatesValidTransactionsWithCorrectKeys() {
        Transaction txn = generator.nextTransaction();

        assertThat(txn.getTxnId()).isNotNull();
        assertThat(txn.getAccountId()).isNotNull();
        assertThat(txn.getAmount()).isGreaterThan(BigDecimal.ZERO);
        assertThat(txn.getAmount().scale()).isLessThanOrEqualTo(2);
        assertThat(txn.getCurrency()).isEqualTo("USD");
        assertThat(txn.getEventTime()).isAfter(Instant.EPOCH);
    }

    @Test
    void highValueShareIsRoughlyTenPercent() {
        List<Transaction> txns = new ArrayList<>();
        for (int i = 0; i < 2_000; i++) {
            txns.add(generator.nextTransaction());
        }
        long highValue = txns.stream().filter(t -> t.getAmount().compareTo(BigDecimal.valueOf(10_000)) > 0).count();
        double share = highValue / (double) txns.size();

        assertThat(share).isBetween(0.05, 0.20);
        txns.stream()
                .filter(t -> t.getAmount().compareTo(BigDecimal.valueOf(10_000)) <= 0)
                .forEach(t -> assertThat(t.getAmount()).isLessThanOrEqualTo(BigDecimal.valueOf(2_001)));
    }

    @Test
    void keysComeFromTheSharedAccountPool() {
        AccountPool pool = new AccountPool(200);
        for (int i = 0; i < 50; i++) {
            Transaction txn = generator.nextTransaction();
            assertThat(pool.all()).anyMatch(a -> a.accountId().equals(txn.getAccountId()));
        }
    }

    @Test
    void sendOneCountsSuccess() {
        generator.sendOne();
        assertThat(generator.getSentCount()).isEqualTo(1);
        assertThat(generator.getFailedCount()).isZero();
    }
}
