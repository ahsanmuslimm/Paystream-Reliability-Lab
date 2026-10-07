package com.paystream.frauddetector.streams;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Pure velocity-window logic (T1): window pruning, one-shot triggering and
 * re-arming after the window slides.
 */
class VelocityRuleTest {

    private static final long WINDOW_MS = 60_000;
    private static final int LIMIT = 5;

    @Test
    void firesExactlyOnTheLimitingTransaction() {
        List<Long> history = new ArrayList<>();
        long base = 1_000_000L;

        for (int i = 0; i < LIMIT - 1; i++) {
            assertThat(VelocityRule.onTransaction(history, base + i, WINDOW_MS, LIMIT)).isFalse();
        }
        assertThat(VelocityRule.onTransaction(history, base + LIMIT - 1, WINDOW_MS, LIMIT)).isTrue();
    }

    @Test
    void doesNotRefireWhileTheBurstContinues() {
        List<Long> history = new ArrayList<>();
        long base = 1_000_000L;

        for (int i = 0; i < LIMIT + 5; i++) {
            boolean fired = VelocityRule.onTransaction(history, base + i, WINDOW_MS, LIMIT);
            if (i == LIMIT - 1) {
                assertThat(fired).isTrue();
            } else {
                assertThat(fired).isFalse();
            }
        }
    }

    @Test
    void prunesTransactionsOutsideTheWindow() {
        List<Long> history = new ArrayList<>();
        long base = 1_000_000L;

        for (int i = 0; i < LIMIT; i++) {
            VelocityRule.onTransaction(history, base + i, WINDOW_MS, LIMIT);
        }
        assertThat(history).hasSize(LIMIT);

        // a transaction a full window later evicts every earlier timestamp
        VelocityRule.onTransaction(history, base + 2 * WINDOW_MS, WINDOW_MS, LIMIT);
        assertThat(history).hasSize(1);
        assertThat(history.get(0)).isEqualTo(base + 2 * WINDOW_MS);
    }

    @Test
    void reArmsAfterTheWindowSlides() {
        List<Long> history = new ArrayList<>();
        long base = 1_000_000L;

        for (int i = 0; i < LIMIT; i++) {
            VelocityRule.onTransaction(history, base + i, WINDOW_MS, LIMIT);
        }
        // new burst starts after the old window has fully expired
        long secondBurst = base + WINDOW_MS + 100;
        for (int i = 0; i < LIMIT; i++) {
            boolean fired = VelocityRule.onTransaction(history, secondBurst + i, WINDOW_MS, LIMIT);
            assertThat(fired).isEqualTo(i == LIMIT - 1);
        }
    }

    @Test
    void boundaryTimestampStaysInsideTheWindow() {
        List<Long> history = new ArrayList<>();
        long base = 1_000_000L;

        VelocityRule.onTransaction(history, base, WINDOW_MS, LIMIT);
        // exactly windowMs later: ts >= eventTime - windowMs, still in window
        VelocityRule.onTransaction(history, base + WINDOW_MS, WINDOW_MS, LIMIT);
        assertThat(history).hasSize(2);
    }
}
