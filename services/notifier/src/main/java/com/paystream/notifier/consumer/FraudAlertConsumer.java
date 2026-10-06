package com.paystream.notifier.consumer;

import bank.events.FraudAlert;
import com.paystream.notifier.service.AlertIngestionService;
import com.paystream.notifier.service.NotificationPublisher;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consumes fraud alerts with manual offset acknowledgement (Document 03
 * section 5.3 consumer baselines: enable.auto.commit=false,
 * CooperativeStickyAssignor). The offset is acknowledged only after the
 * idempotent ingest committed, giving at-least-once delivery with
 * exactly-once effects (FR-06).
 */
@Component
public class FraudAlertConsumer {

    private final AlertIngestionService ingestion;
    private final NotificationPublisher publisher;

    public FraudAlertConsumer(AlertIngestionService ingestion, NotificationPublisher publisher) {
        this.ingestion = ingestion;
        this.publisher = publisher;
    }

    @KafkaListener(
            topics = com.paystream.common.Topics.FRAUD_ALERTS,
            containerFactory = "fraudAlertContainerFactory")
    public void onAlert(ConsumerRecord<String, FraudAlert> record, Acknowledgment ack) {
        AlertIngestionService.IngestResult result =
                ingestion.ingest(record.value(), record.topic(), record.partition(), record.offset());
        if (result.ingested()) {
            publisher.publish(result.customerId(), result.alertId(), record.value().getSeverity().name());
        }
        ack.acknowledge();
    }
}
