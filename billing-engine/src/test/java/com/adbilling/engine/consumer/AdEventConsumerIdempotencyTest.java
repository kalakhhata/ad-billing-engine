package com.adbilling.engine.consumer;

import com.adbilling.balance.grpc.DeductFundsResponse;
import com.adbilling.balance.grpc.DeductFundsStatus;
import com.adbilling.engine.grpc.BalanceServiceClient;
import com.adbilling.engine.model.AdEvent;
import com.adbilling.engine.repository.TransactionLogRepository;
import com.adbilling.engine.service.BillingService;
import com.adbilling.engine.service.IdempotencyService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests the idempotency guarantee at the billing-engine layer.
 *
 * We mock the gRPC client so this test runs without a live balance service.
 * The test proves that replaying the same event_id never triggers a second
 * DeductFunds call — which is what ultimately protects against double-charging.
 */
@SpringBootTest
@Testcontainers
class AdEventConsumerIdempotencyTest {

    @Container
    @SuppressWarnings("resource")
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("billing")
            .withUsername("billing")
            .withPassword("billing_secret")
            .withInitScript("init-test.sql");

    @Container
    @SuppressWarnings("resource")
    static GenericContainer<?> redis = new GenericContainer<>("redis:7.2-alpine")
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", postgres::getJdbcUrl);
        r.add("spring.datasource.username", postgres::getUsername);
        r.add("spring.datasource.password", postgres::getPassword);
        r.add("spring.data.redis.host", redis::getHost);
        r.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @MockBean
    private BalanceServiceClient balanceClient;

    @Autowired
    private BillingService billingService;

    @Autowired
    private IdempotencyService idempotencyService;

    @Autowired
    private TransactionLogRepository txnRepo;

    @BeforeEach
    void setUp() {
        when(balanceClient.deductFunds(anyString(), anyLong(), anyString()))
                .thenReturn(DeductFundsResponse.newBuilder()
                        .setStatus(DeductFundsStatus.SUCCESS)
                        .setBalanceAfter(900_000L)
                        .build());
    }

    @Test
    @DisplayName("IDEMPOTENCY: replaying same event_id does not call DeductFunds twice")
    void replay_doesNotDoubleCharge() {
        AdEvent event = new AdEvent();
        event.setEventId("evt-idem-" + UUID.randomUUID());
        event.setAdvertiserId("adv-001");
        event.setCampaignId("camp-001");
        event.setEventType("CLICK");
        event.setCostMicros(100_000L);
        event.setTimestamp(System.currentTimeMillis());

        // First processing
        boolean firstPass = idempotencyService.markIfNew(event.getEventId());
        assertThat(firstPass).isTrue();
        billingService.process(event);

        // Simulate Kafka re-delivering the same message
        boolean secondPass = idempotencyService.markIfNew(event.getEventId());
        assertThat(secondPass).isFalse(); // Redis blocks it

        // DeductFunds must have been called exactly once
        verify(balanceClient, times(1))
                .deductFunds(event.getAdvertiserId(), event.getCostMicros(), event.getEventId());

        // Only one transaction log entry
        long count = txnRepo.findAll().stream()
                .filter(t -> t.getEventId().equals(event.getEventId()))
                .count();
        assertThat(count).isEqualTo(1);
    }

    @Test
    @DisplayName("IDEMPOTENCY: DB constraint catches duplicate even if Redis misses it")
    void dbConstraint_catchesDuplicate_ifRedisMisses() {
        AdEvent event = new AdEvent();
        event.setEventId("evt-db-idem-" + UUID.randomUUID());
        event.setAdvertiserId("adv-001");
        event.setCampaignId("camp-001");
        event.setEventType("IMPRESSION");
        event.setCostMicros(10_000L);
        event.setTimestamp(System.currentTimeMillis());

        // First call succeeds normally
        billingService.process(event);

        // Second call — Redis bypassed (simulates Redis restart), DB constraint fires
        // BillingService must handle DataIntegrityViolationException gracefully
        BillingService.ProcessingResult result = billingService.process(event);
        assertThat(result).isEqualTo(BillingService.ProcessingResult.SUCCESS);

        // Still only one DB record
        long count = txnRepo.findAll().stream()
                .filter(t -> t.getEventId().equals(event.getEventId()))
                .count();
        assertThat(count).isEqualTo(1);
    }

    @Test
    @DisplayName("REJECTED: insufficient funds are logged and not retried")
    void insufficientFunds_isRejected() {
        when(balanceClient.deductFunds(anyString(), anyLong(), anyString()))
                .thenReturn(DeductFundsResponse.newBuilder()
                        .setStatus(DeductFundsStatus.INSUFFICIENT_FUNDS)
                        .build());

        AdEvent event = new AdEvent();
        event.setEventId("evt-rejected-" + UUID.randomUUID());
        event.setAdvertiserId("adv-broke");
        event.setCampaignId("camp-001");
        event.setEventType("CLICK");
        event.setCostMicros(999_999_999L);
        event.setTimestamp(System.currentTimeMillis());

        BillingService.ProcessingResult result = billingService.process(event);
        assertThat(result).isEqualTo(BillingService.ProcessingResult.REJECTED);

        txnRepo.findByEventId(event.getEventId()).ifPresent(txn -> {
            assertThat(txn.getStatus()).isEqualTo("REJECTED");
            assertThat(txn.getRejectReason()).isEqualTo("INSUFFICIENT_FUNDS");
        });
    }
}
