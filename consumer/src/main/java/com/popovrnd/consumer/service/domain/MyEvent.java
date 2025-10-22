package com.popovrnd.consumer.service.domain;

import java.io.Serializable;
import java.util.Date;
import java.util.UUID;

/**
 * A lean, production-style event DTO for Kafka or any streaming system.
 */
public record MyEvent(
        String id,           // Unique event ID for traceability
        String type,         // Event type, e.g. "USER_CREATED", "JOB_STARTED"
        String source,       // Origin service or module
        Object payload,      // Actual data (can be DTO, Map, or simple text)
        Date createdAt,      // Event creation timestamp
        String correlationId // Optional, to link related messages
) implements Serializable {

    public static MyEvent of(String type, String source, Object payload, String correlationId) {
        return new MyEvent(
                UUID.randomUUID().toString(),
                type,
                source,
                payload,
                new Date(),
                correlationId
        );
    }
}

