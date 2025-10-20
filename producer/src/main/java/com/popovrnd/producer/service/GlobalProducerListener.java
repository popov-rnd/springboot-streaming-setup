package com.popovrnd.producer.service;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.stereotype.Component;

@Component
public class GlobalProducerListener implements ProducerListener<Object, Object> {

    private static final Logger log = LoggerFactory.getLogger(GlobalProducerListener.class);

    @Override
    public void onSuccess(ProducerRecord<Object, Object> record, RecordMetadata metadata) {
        log.debug("✅ Sent successfully: topic={} offset={}", record.topic(), metadata.offset());
    }

    /**
     * Invoked when a Kafka record is not acknowledged by the broker after all configured retries.
     * <p>
     * Executed <b>once per message</b>, even if the Kafka producer sent multiple messages in
     * a single internal batch. Each failed {@link ProducerRecord} triggers its own
     * {@code onError} callback.
     * <p>
     * This callback must stay <b>non-blocking</b>; never perform disk or network I/O here.
     * Heavy recovery work should be delegated to background workers or a dedicated
     * re-sender component.
     * <p>
     * Recommended hybrid reliability pattern:
     * <pre>
     * Producer send()
     *     ↓
     *  In-memory queue      ← absorbs short hiccups (seconds)
     *     ↓
     *  Durable spool/outbox ← guarantees persistence during longer outages (minutes–hours)
     *     ↓
     *  Kafka broker
     * </pre>
     * The in-memory queue provides short-term shock absorption for transient network
     * or broker interruptions, while the durable spool (e.g. Chronicle Queue or a DB
     * outbox) ensures no data loss if Kafka remains unavailable for an extended period.
     * <p>
     * Within this model, {@code onError} should only:
     * <ul>
     *   <li>🪵 Log the failure with topic, key, and exception details (structured logging recommended).</li>
     *   <li>💾 Push the record into the in-memory buffer for deferred retry.</li>
     *   <li>📈 Increment a metrics counter (e.g. Micrometer/Prometheus) to track send failures.</li>
     *   <li>🚨 Optionally trigger a circuit-breaker when consecutive failures exceed a threshold.</li>
     * </ul>
     * The background re-sender component is responsible for draining the buffer,
     * persisting to disk when necessary, and re-publishing to Kafka or a
     * dead-letter topic once connectivity is restored.
     *
     * @param record   the failed Kafka record (topic, key, and payload)
     * @param metadata record metadata if available; may be {@code null} on network-level failure
     * @param ex       the exception describing the cause of the failure
     */
    @Override
    public void onError(ProducerRecord<Object, Object> record,
                        RecordMetadata metadata, Exception ex) {
        log.error("❌ Failed to send topic={} key={}", record.topic(), record.key(), ex);
    }
}

