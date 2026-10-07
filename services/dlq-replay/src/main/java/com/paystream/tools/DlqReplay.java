package com.paystream.tools;

import java.io.FileInputStream;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * dlq-replay - operator tool that replays dead-lettered records back to their
 * original topics (runbook: docs/runbooks/dlq-replay.md).
 *
 * <p>The DLQ value is the original raw bytes (Document 03 section 3), so
 * replay re-publishes exactly what failed. Target selection uses the
 * {@code dlq.original.topic} header unless --target is given; a common
 * operator pattern is replaying into the matching {@code .retry} topic for
 * deferred reprocessing instead of straight back to production traffic.
 *
 * <p>Usage:
 * <pre>
 * java -jar dlq-replay-0.1.0.jar \
 *   --bootstrap localhost:19092 \
 *   --dlq bank.transactions.v1.dlq \
 *   [--target bank.transactions.v1.retry] \
 *   [--limit 100] [--dry-run]
 *   [--config client.properties]
 * </pre>
 * {@code --config} points at Kafka client properties for authenticated
 * clusters (SASL_SSL, e.g. the svc-fraud credentials, which the ACL matrix
 * grants READ on the .dlq topics and WRITE on the business topics).
 */
public final class DlqReplay {

    private static final Logger log = LoggerFactory.getLogger(DlqReplay.class);

    private DlqReplay() {
    }

    public static void main(String[] args) throws Exception {
        String bootstrap = null;
        String dlqTopic = null;
        String target = null;
        String configFile = null;
        long limit = Long.MAX_VALUE;
        boolean dryRun = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--help", "-h" -> {
                    printHelp();
                    return;
                }
                case "--bootstrap" -> bootstrap = args[++i];
                case "--dlq" -> dlqTopic = args[++i];
                case "--target" -> target = args[++i];
                case "--limit" -> limit = Long.parseLong(args[++i]);
                case "--dry-run" -> dryRun = true;
                case "--config" -> configFile = args[++i];
                default -> {
                    System.err.println("Unknown option: " + args[i]);
                    printHelp();
                    System.exit(2);
                }
            }
        }
        if (bootstrap == null || dlqTopic == null) {
            System.err.println("--bootstrap and --dlq are required");
            printHelp();
            System.exit(2);
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("group.id", "dlq-replay");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        if (configFile != null) {
            try (var in = new FileInputStream(configFile)) {
                props.load(in);
            }
        }

        Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        Producer<byte[], byte[]> producer = new KafkaProducer<>(props,
                new ByteArraySerializer(), new ByteArraySerializer());

        try {
            ReplayService service = new ReplayService(consumer, producer, dlqTopic, target, dryRun);
            ReplayService.Summary summary = service.replay(limit);
            String mode = dryRun ? " (dry run)" : "";
            System.out.printf("Replay from %s%s: %d replayed, %d skipped, %d failed%n",
                    dlqTopic, mode, summary.replayed(), summary.skipped(), summary.failed());
            if (summary.failed() > 0 && !dryRun) {
                System.exit(1);
            }
        } finally {
            consumer.close();
            producer.close();
        }
    }

    private static void printHelp() {
        System.out.println("""
                dlq-replay - replay dead-lettered records to their original topics

                Options:
                  --bootstrap URL       Kafka bootstrap servers (required)
                  --dlq TOPIC           DLQ topic to drain (required)
                  --target TOPIC        Republish target (default: dlq.original.topic header)
                  --limit N             Max records this run processes
                  --dry-run             Count what would be replayed without producing
                  --config FILE         Client properties (security, SASL_SSL principals)
                """);
    }
}
