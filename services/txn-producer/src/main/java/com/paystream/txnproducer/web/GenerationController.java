package com.paystream.txnproducer.web;

import com.paystream.txnproducer.generator.SyntheticTransactionGenerator;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Operations API for the synthetic workload: start/stop/status. Rate is
 * clamped to the FR-04 range of 1 to 5,000 msg/s.
 */
@RestController
@RequestMapping("/api/generation")
public class GenerationController {

    private final SyntheticTransactionGenerator generator;

    public GenerationController(SyntheticTransactionGenerator generator) {
        this.generator = generator;
    }

    @PostMapping("/start")
    public ResponseEntity<Map<String, Object>> start(@RequestParam int rate) {
        if (rate < SyntheticTransactionGenerator.MIN_RATE || rate > SyntheticTransactionGenerator.MAX_RATE) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "rate must be between "
                            + SyntheticTransactionGenerator.MIN_RATE + " and "
                            + SyntheticTransactionGenerator.MAX_RATE));
        }
        generator.start(rate);
        return ResponseEntity.ok(status());
    }

    @PostMapping("/stop")
    public Map<String, Object> stop() {
        generator.stop();
        return status();
    }

    @GetMapping("/status")
    public Map<String, Object> getStatus() {
        return status();
    }

    private Map<String, Object> status() {
        return Map.of(
                "running", generator.isRunning(),
                "ratePerSecond", generator.getRatePerSecond(),
                "sent", generator.getSentCount(),
                "failed", generator.getFailedCount());
    }
}
