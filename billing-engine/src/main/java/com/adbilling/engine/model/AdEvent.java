package com.adbilling.engine.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

/**
 * Kafka message schema for ad events.
 * costMicros: cost in micro-dollars (1 USD = 1,000,000 micros).
 * Using integer arithmetic throughout avoids floating-point rounding bugs.
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class AdEvent {
    private String eventId;
    private String advertiserId;
    private String campaignId;
    private String eventType;   // CLICK | IMPRESSION
    private long   costMicros;
    private long   timestamp;
    private int    retryCount;
}
