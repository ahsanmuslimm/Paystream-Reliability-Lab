package com.paystream.common;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Deterministic synthetic account pool shared by every service.
 *
 * txn-producer generates transactions for these accounts; notifier seeds the
 * same accounts into PostgreSQL (idempotent, ON-CONFLICT-style upsert) so the
 * fraud_alerts foreign keys hold without real customer data. Determinism is
 * what keeps producer, consumer and database in agreement without sharing a
 * state store.
 */
public final class AccountPool {

    private final List<Account> accounts;

    public record Account(UUID accountId, UUID customerId, int index) {
    }

    public AccountPool(int size) {
        accounts = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            accounts.add(new Account(
                    UUID.nameUUIDFromBytes(("paystream-account-" + i).getBytes()),
                    UUID.nameUUIDFromBytes(("paystream-customer-" + i).getBytes()),
                    i));
        }
    }

    public Account get(int index) {
        return accounts.get(Math.floorMod(index, accounts.size()));
    }

    public int size() {
        return accounts.size();
    }

    public List<Account> all() {
        return List.copyOf(accounts);
    }
}
