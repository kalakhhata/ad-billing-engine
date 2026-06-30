package com.adbilling.engine.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

/**
 * Prevents duplicate processing of Kafka messages at the billing-engine layer.
 *
 * WHY REDIS HERE?
 * PostgreSQL's UNIQUE constraint on event_id is the durable, authoritative guard.
 * Redis is a fast pre-check that short-circuits before we make a gRPC call.
 * The two layers together provide defense-in-depth:
 *   - Redis: fast (~0.1ms) in-memory dedup check for hot path
 *   - PostgreSQL UNIQUE: durable, survives Redis restart
 *
 * If Redis is unavailable we let the request through; the DB constraint catches it.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyService {

    private final StringRedisTemplate redisTemplate;

    private static final String PREFIX  = "billing:processed:";
    private static final Duration TTL   = Duration.ofDays(7);

    /**
     * Attempts to mark the event as being processed.
     * Uses SETNX (SET if Not eXists) — atomic in Redis.
     *
     * @return true if this is a new event (should be processed),
     *         false if it was already seen (duplicate — skip it)
     */
    public boolean markIfNew(String eventId) {
        Boolean isNew = redisTemplate.opsForValue()
                .setIfAbsent(PREFIX + eventId, "1", TTL);
        if (Boolean.FALSE.equals(isNew)) {
            log.debug("Duplicate event detected (Redis): {}", eventId);
        }
        return Boolean.TRUE.equals(isNew);
    }

    public void unmark(String eventId) {
        // Called on processing failure so the event can be retried
        redisTemplate.delete(PREFIX + eventId);
    }
}
