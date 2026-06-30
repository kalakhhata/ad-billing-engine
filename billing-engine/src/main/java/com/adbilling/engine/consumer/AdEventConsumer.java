package com.adbilling.engine.consumer;

import com.adbilling.engine.model.AdEvent;
import com.adbilling.engine.service.BillingService;
import com.adbilling.engine.service.BillingService.ProcessingResult;
import com.adbilling.engine.service.IdempotencyService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Kafka consumer that processes ad events from the ad-events topic.
 *
 * DELIVERY SEMANTICS:
 * Spring Kafka is configured with manual ACK mode (AckMode.MANUAL_IMMEDIATE).
 * We only commit the offset after successful processing — this gives us
 * at-least-once delivery. Combined with idempotency checks, we get
 * effectively-once processing for the happy path.
 *
 * WHY AT-LEAST-ONCE (not exactly-once)?
 * Kafka's exactly-once (EOS) requires transactions and adds ~20-30% overhead.
 * For billing, at-least-once + idempotency is the industry-standard approach
 * (Stripe, Braintree all use this pattern). Exactly-once is strictly harder
 * and the marginal safety gain is offset by the complexity and latency cost.
 *
 * DLQ + RETRY PATTERN:
 * Transient errors (gRPC unavailable, network blip) trigger exponential
 * backoff retries up to MAX_RETRIES. After that, the event is routed to
 * the DLQ for human review / offline reprocessing — we never silently drop
 * a failed event.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AdEventConsumer {

    private final BillingService billingService;
    private final IdempotencyService idempotencyService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${billing.kafka.topics.dlq}")
    private String dlqTopic;

    @Value("${billing.retry.max-attempts:3}")
    private int maxRetries;

    @Value("${billing.retry.initial-backoff-ms:500}")
    private long initialBackoffMs;

    @Value("${billing.retry.backoff-multiplier:2.0}")
    private double backoffMultiplier;

    @KafkaListener(
            topics = "${billing.kafka.topics.ad-events}",
            groupId = "${spring.kafka.consumer.group-id}",
            concurrency = "3"
    )
    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
        AdEvent event = null;
        try {
            event = objectMapper.readValue(record.value(), AdEvent.class);
        } catch (Exception e) {
            log.error("Failed to deserialize message from topic={} offset={}: {}",
                    record.topic(), record.offset(), e.getMessage());
            ack.acknowledge(); // Bad message, can't retry — ack to skip
            return;
        }

        // Fast path: Redis idempotency pre-check
        if (!idempotencyService.markIfNew(event.getEventId())) {
            log.debug("Skipping duplicate event (Redis cache): {}", event.getEventId());
            ack.acknowledge();
            return;
        }

        processWithRetry(event, ack, record.value());
    }

    private void processWithRetry(AdEvent event, Acknowledgment ack, String rawMessage) {
        long backoffMs = initialBackoffMs;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            ProcessingResult result = billingService.process(event);

            if (result == ProcessingResult.SUCCESS || result == ProcessingResult.REJECTED) {
                ack.acknowledge();
                return;
            }

            // RETRY path
            if (attempt < maxRetries) {
                log.warn("Retrying event={} attempt={}/{} backoff={}ms",
                        event.getEventId(), attempt, maxRetries, backoffMs);
                sleep(backoffMs);
                backoffMs = (long) (backoffMs * backoffMultiplier); // exponential backoff
                event.setRetryCount(attempt);
            }
        }

        // Max retries exceeded — route to DLQ
        log.error("Max retries exceeded for event={}, routing to DLQ", event.getEventId());
        routeToDlq(event, rawMessage);
        idempotencyService.unmark(event.getEventId()); // allow future reprocessing from DLQ
        ack.acknowledge();
    }

    private void routeToDlq(AdEvent event, String rawMessage) {
        try {
            kafkaTemplate.send(dlqTopic, event.getEventId(), rawMessage);
            log.info("Event {} sent to DLQ topic={}", event.getEventId(), dlqTopic);
        } catch (Exception e) {
            log.error("Failed to send event {} to DLQ: {}", event.getEventId(), e.getMessage());
        }
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
