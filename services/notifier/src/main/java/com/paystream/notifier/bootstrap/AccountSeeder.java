package com.paystream.notifier.bootstrap;

import com.paystream.common.AccountPool;
import com.paystream.notifier.domain.AccountEntity;
import com.paystream.notifier.repository.AccountRepository;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Seeds the deterministic synthetic account pool into PostgreSQL so the
 * fraud_alerts foreign keys hold. Idempotent: rows carry fixed primary keys
 * and are only inserted when missing, so restarts and replays are safe.
 */
@Component
public class AccountSeeder implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(AccountSeeder.class);

    private final AccountRepository accounts;
    private final AccountPool accountPool;

    public AccountSeeder(AccountRepository accounts, AccountPool accountPool) {
        this.accounts = accounts;
        this.accountPool = accountPool;
    }

    @Override
    @Transactional
    public void run(ApplicationArguments args) {
        List<AccountEntity> missing = new ArrayList<>();
        for (AccountPool.Account account : accountPool.all()) {
            if (!accounts.existsById(account.accountId())) {
                java.math.BigDecimal balance = java.math.BigDecimal.valueOf(1_000L + 137L * account.index(), 2);
                missing.add(new AccountEntity(account.accountId(), account.customerId(), balance, "ACTIVE"));
            }
        }
        if (!missing.isEmpty()) {
            accounts.saveAll(missing);
            log.info("Seeded {} synthetic accounts (pool size {})", missing.size(), accountPool.size());
        }
    }
}
