package com.adbilling.balance.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests using a real Redis via Testcontainers.
 * These tests prove two critical properties:
 *   1. Idempotency: replaying the same idempotency key does NOT double-deduct.
 *   2. Concurrency safety: concurrent deductions never allow overspend.
 */
@SpringBootTest
@Testcontainers
class BalanceServiceTest {

    @Container
    @SuppressWarnings("resource")
    static GenericContainer<?> redis = new GenericContainer<>("redis:7.2-alpine")
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @Autowired
    private BalanceService balanceService;

    private static final String ADV_ID = "test-adv-001";
    private static final long   INITIAL_BALANCE = 1_000_000L; // 1 USD in micros

    @BeforeEach
    void setUp() {
        balanceService.seedBalance(ADV_ID, INITIAL_BALANCE);
    }

    @Test
    @DisplayName("CheckBalance returns seeded value")
    void checkBalance_returnsSeededValue() {
        assertThat(balanceService.getBalance(ADV_ID)).isEqualTo(INITIAL_BALANCE);
    }

    @Test
    @DisplayName("DeductFunds: successful deduction reduces balance correctly")
    void deductFunds_success() {
        long amountMicros = 100_000L;
        String idemKey = UUID.randomUUID().toString();

        long result = balanceService.deductFunds(ADV_ID, amountMicros, idemKey);

        assertThat(result).isEqualTo(INITIAL_BALANCE - amountMicros);
        assertThat(balanceService.getBalance(ADV_ID)).isEqualTo(INITIAL_BALANCE - amountMicros);
    }

    @Test
    @DisplayName("IDEMPOTENCY: replaying the same event key does NOT double-deduct")
    void deductFunds_idempotency_nodoublecharg() {
        long amountMicros = 100_000L;
        String idemKey = "event-idem-" + UUID.randomUUID();

        // First call: should succeed
        long first = balanceService.deductFunds(ADV_ID, amountMicros, idemKey);
        assertThat(first).isEqualTo(INITIAL_BALANCE - amountMicros);

        // Second call with same key: should return ALREADY_PROCESSED (-2)
        long second = balanceService.deductFunds(ADV_ID, amountMicros, idemKey);
        assertThat(second).isEqualTo(-2L);

        // Balance must not have changed after the replay
        assertThat(balanceService.getBalance(ADV_ID)).isEqualTo(INITIAL_BALANCE - amountMicros);
    }

    @Test
    @DisplayName("DeductFunds: insufficient funds returns 0")
    void deductFunds_insufficientFunds() {
        long tooMuch = INITIAL_BALANCE + 1;
        long result = balanceService.deductFunds(ADV_ID, tooMuch, UUID.randomUUID().toString());
        assertThat(result).isEqualTo(0L);
        // Balance must be unchanged
        assertThat(balanceService.getBalance(ADV_ID)).isEqualTo(INITIAL_BALANCE);
    }

    @Test
    @DisplayName("DeductFunds: unknown advertiser returns -1")
    void deductFunds_advertiserNotFound() {
        long result = balanceService.deductFunds("unknown-adv", 1000L, UUID.randomUUID().toString());
        assertThat(result).isEqualTo(-1L);
    }

    @Test
    @DisplayName("CONCURRENCY: 50 concurrent deductions never allow overspend")
    void deductFunds_concurrent_noOverspend() throws InterruptedException {
        // Budget: 10 USD = 10,000,000 micros; each deduction is 300,000 micros (0.30 USD)
        // Max that should fit: 33 deductions (33 * 300,000 = 9,900,000)
        long budget       = 10_000_000L;
        long costPerEvent = 300_000L;
        int  threads      = 50;

        balanceService.seedBalance(ADV_ID, budget);

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch  latch    = new CountDownLatch(1);
        List<Future<Long>> futures = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            final String idemKey = "concurrent-" + i;
            futures.add(executor.submit(() -> {
                latch.await();
                return balanceService.deductFunds(ADV_ID, costPerEvent, idemKey);
            }));
        }

        latch.countDown(); // release all threads simultaneously
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        AtomicInteger successes = new AtomicInteger(0);
        AtomicInteger rejected  = new AtomicInteger(0);

        for (Future<Long> f : futures) {
            try {
                long res = f.get();
                if (res > 0) successes.incrementAndGet();
                else if (res == 0) rejected.incrementAndGet();
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        long finalBalance = balanceService.getBalance(ADV_ID);

        // Balance must never go negative
        assertThat(finalBalance).isGreaterThanOrEqualTo(0L);

        // Verify accounting: initial - (successes * cost) == finalBalance
        assertThat(budget - ((long) successes.get() * costPerEvent)).isEqualTo(finalBalance);

        System.out.printf("Concurrency test: %d/%d deductions succeeded, %d rejected, " +
                "final balance = %d micros%n", successes.get(), threads, rejected.get(), finalBalance);
    }
}
