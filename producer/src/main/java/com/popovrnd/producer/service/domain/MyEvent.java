package com.popovrnd.producer.service.domain;

import java.io.Serializable;
import java.util.Date;
import java.util.UUID;

/**
 * A lean, production-style event DTO for Kafka or any streaming system.
 *  - id: unique per event, used for idempotency & Kafka message key.
 *  - correlationId: shared across events in the same request/transaction flow for tracing.
 */
public record MyEvent(
        String id,           // Unique event ID for idempotency & Kafka message key.
        String type,         // Event type, e.g. "USER_CREATED", "JOB_STARTED"
        String source,       // Origin service or module
        Object payload,      // Actual data (can be DTO, Map, or simple text)
        Date createdAt,      // Event creation timestamp
        String correlationId // Optional, shared across events in the same request/transaction flow for tracing.
) implements Serializable {

    public static MyEvent of(String type, String source, Object payload) {
        return new MyEvent(
                UUID.randomUUID().toString(),
                type,
                source,
                payload,
                new Date(),
                UUID.randomUUID().toString()
        );
    }
}

