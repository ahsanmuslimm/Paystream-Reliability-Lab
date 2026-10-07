package com.paystream.frauddetector.streams;

import java.util.ArrayList;
import java.util.List;

/**
 * Pure velocity-window logic (T1/T5 test target): an account that reaches
 * {@code limit} transactions inside a trailing {@code windowMs} window of
 * event time is suspicious.
 *
 * <p>The rule fires exactly once per burst - on the transaction that brings
 * the in-window count up to the limit. Subsequent transactions keep the
 * count above the limit and do not re-fire; once enough of the window has
 * expired, a new burst can trigger again. Event-time semantics come from the
 * record timestamp, so replays produce the same decisions (D6/T5).
 */
public final class VelocityRule {

    private VelocityRule() {
    }

    /**
     * Adds the timestamp to the in-window history and decides whether the
     * limit is reached. Mutates and returns the pruned history so callers can
     * persist exactly what was evaluated.
     *
     * @return true when this transaction is the Nth one inside the window
     */
    public static boolean onTransaction(List<Long> history, long eventTimeMs,
                                        long windowMs, int limit) {
        history.removeIf(ts -> ts < eventTimeMs - windowMs);
        history.add(eventTimeMs);
        return history.size() == limit;
    }

    /** Copy-free helper for the state store round trip. */
    public static List<Long> copy(List<Long> history) {
        return history == null ? new ArrayList<>() : new ArrayList<>(history);
    }
}
