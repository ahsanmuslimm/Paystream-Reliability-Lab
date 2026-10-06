package com.paystream.txnproducer.config;

import com.paystream.txnproducer.generator.SyntheticTransactionGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

/**
 * Starts the synthetic workload automatically at the configured rate so that
 * `make up` results in a live platform without extra steps (FR-04, G1).
 */
@Configuration
public class AutoStartConfig {

    private static final Logger log = LoggerFactory.getLogger(AutoStartConfig.class);

    private final SyntheticTransactionGenerator generator;
    private final boolean autoStart;
    private final int initialRate;

    public AutoStartConfig(SyntheticTransactionGenerator generator,
                           @Value("${paystream.produce.auto-start:true}") boolean autoStart,
                           @Value("${paystream.produce.rate-per-second:100}") int initialRate) {
        this.generator = generator;
        this.autoStart = autoStart;
        this.initialRate = initialRate;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        if (autoStart) {
            log.info("Auto-starting generation at {} msg/s", initialRate);
            generator.start(initialRate);
        } else {
            log.info("Auto-start disabled; call POST /api/generation/start?rate=N to begin");
        }
    }
}
