package com.popovrnd.producer.web.request;

import jakarta.validation.constraints.*;

/**
 * Represents a job submitted via HTTP API.
 */
public record MyEventRequest(
        @NotBlank String type,        // Logical message type, e.g. "DATA_EXPORT"
        @NotBlank String source,      // Origin service/module (optional for external clients)
        @NotNull Object payload       // Core job data — can be any structured object
) {}
