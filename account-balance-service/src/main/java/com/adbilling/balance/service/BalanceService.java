package com.adbilling.balance.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Owns the balance store in Redis.
 *
 * Key schema:
 *   balance:{advertiserId}          -> Long (micros remaining)
 *   deduct:processed:{idempotencyKey} -> 1   (idempotency marker, TTL 7 days)
 *
 * DeductFunds uses a Lua script so the "check balance → deduct" path is
 * atomic from Redis's perspective.  Redis executes scripts single-threaded,
 * so there is no TOCTOU race between checking and deducting — this is the
 * correct solution for preventing overspen under concurrent load.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class BalanceService {

    private final RedisTemplate<String, Long> redisTemplate;

    private static final String BALANCE_KEY_PREFIX  = "balance:";
    private static final String DEDUCT_IDEM_PREFIX  = "deduct:processed:";
    private static final long   IDEM_TTL_SECONDS    = 7 * 24 * 3600L;

    /**
     * Lua script for atomic check-and-deduct.
     *
     * Returns:
     *   -2  -> idempotency key already processed (ALREADY_PROCESSED)
     *   -1  -> advertiser not found
     *    0  -> insufficient funds
     *   >0  -> balance after deduction (SUCCESS)
     */
    private static final String DEDUCT_LUA_SCRIPT = """
        local idem_key    = KEYS[1]
        local balance_key = KEYS[2]
        local amount      = tonumber(ARGV[1])
        local ttl         = tonumber(ARGV[2])

        -- Idempotency: reject if this deduction was already processed
        if redis.call('EXISTS', idem_key) == 1 then
            return -2
        end

        -- Check advertiser exists
        local balance = redis.call('GET', balance_key)
        if not balance then
            return -1
        end

        balance = tonumber(balance)

        -- Check sufficient funds
        if balance < amount then
            return 0
        end

        -- Atomically deduct and mark idempotency key
        local new_balance = balance - amount
        redis.call('SET', balance_key, new_balance)
        redis.call('SETEX', idem_key, ttl, '1')

        return new_balance
        """;

    private final DefaultRedisScript<Long> deductScript = new DefaultRedisScript<>(DEDUCT_LUA_SCRIPT, Long.class);

    public Long getBalance(String advertiserId) {
        return redisTemplate.opsForValue().get(BALANCE_KEY_PREFIX + advertiserId);
    }

    public void seedBalance(String advertiserId, long balanceMicros) {
        redisTemplate.opsForValue().set(BALANCE_KEY_PREFIX + advertiserId, balanceMicros);
        log.info("Seeded balance for {} -> {} micros", advertiserId, balanceMicros);
    }

    /**
     * @return negative codes or new balance; see Lua script comments
     */
    public long deductFunds(String advertiserId, long amountMicros, String idempotencyKey) {
        String idemKey    = DEDUCT_IDEM_PREFIX + idempotencyKey;
        String balanceKey = BALANCE_KEY_PREFIX + advertiserId;

        Long result = redisTemplate.execute(
                deductScript,
                List.of(idemKey, balanceKey),
                amountMicros,
                IDEM_TTL_SECONDS
        );

        long value = (result != null) ? result : -1L;
        log.debug("DeductFunds advertiser={} amount={} key={} -> {}",
                advertiserId, amountMicros, idempotencyKey, value);
        return value;
    }
}
