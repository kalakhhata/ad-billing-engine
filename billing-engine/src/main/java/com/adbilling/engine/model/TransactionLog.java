package com.adbilling.engine.model;

import jakarta.persistence.*;
import lombok.*;
import java.time.Instant;

@Entity
@Table(name = "transaction_log")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransactionLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, unique = true)
    private String eventId;

    @Column(name = "advertiser_id", nullable = false)
    private String advertiserId;

    @Column(name = "campaign_id", nullable = false)
    private String campaignId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "cost_micros", nullable = false)
    private long costMicros;

    /**
     * SUCCESS | REJECTED | DLQ
     * REJECTED = insufficient funds (not retried; budget is genuinely exhausted)
     * DLQ      = transient error; routed to dead-letter queue after max retries
     */
    @Column(name = "status", nullable = false)
    private String status;

    @Column(name = "reject_reason")
    private String rejectReason;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;
}
