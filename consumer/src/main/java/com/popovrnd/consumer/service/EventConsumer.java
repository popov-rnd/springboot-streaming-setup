package com.popovrnd.consumer.service;

import com.popovrnd.consumer.service.domain.MyEvent;
import com.popovrnd.consumer.service.domain.exceptions.NonRetryableBusinessException;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

@Component
public class EventConsumer {

    private static final Logger log = LoggerFactory.getLogger(EventConsumer.class);

    @RetryableTopic(
            attempts = "4",             // 1 original attempt + 3 retries (non-blocking). Total = 4 delivery attempts.
            backoff = @Backoff(         // Poisoned messages is re-tired N times with this defined back-off, before being isolated in DLT
                    delay = 500,
                    multiplier = 2,
                    maxDelay = 5000),
            autoCreateTopics = "true",                         // R&D/dev: let Spring auto-create retry/DLT topics. For prod → false (topics pre-created by infra/ops).
            dltTopicSuffix = "-dlt",                           // DLT (Dead Letter Topic) suffix. By default, Spring appends "-dlt", so this is optional but explicit.
            exclude = {NonRetryableBusinessException.class}    // Send validation-like issues straight to DLT
    )
    @KafkaListener(topics = "${consumer.kafka.topic}")
    public void onEvent(
            @Valid @Payload MyEvent event,                     // The deserialized message payload. Validation annotations (@Valid) ensure schema integrity. This is what your business logic actually processes.
            ConsumerRecordMetadata meta,                       // Lightweight access to Kafka metadata (topic, partition, offset, timestamp). Useful for logging, tracing, and observability — not required for normal flow.
            @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key) {  // Optional Kafka message key (used for partitioning and ordering). Useful for correlation or debugging; often equals event.id(). Can be safely omitted if not needed.

        // Trace metadata for observability
        // This is a guard to prevent building the log message (and interpolating parameters) when debug logging is disabled.
        // For simple use-cases like this one the check is redundant.
        if (log.isDebugEnabled()) {
            log.debug("Received an event topic={} partition={} offset={} key={}",
                    meta.topic(), meta.partition(), meta.offset(), key);
        }

        // Business logic (idempotent!)
        if (keyIsNotProcessed(key)) {
            handle(event);
        }

    }

    // Idempotency check
    private boolean keyIsNotProcessed(String key) {
        // TODO: implement persistence-based deduplication (e.g., Redis/DB)
        return true;
    }

    // Keep idempotent: e.id() should be used to guard downstream writes
    private void handle(MyEvent e) {
        log.info("Processed event {} type={} at {}", e.id(), e.type(), e.createdAt());
    }
}
