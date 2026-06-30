package com.adbilling.engine.service;

import com.adbilling.balance.grpc.DeductFundsResponse;
import com.adbilling.balance.grpc.DeductFundsStatus;
import com.adbilling.engine.grpc.BalanceServiceClient;
import com.adbilling.engine.model.AdEvent;
import com.adbilling.engine.model.TransactionLog;
import com.adbilling.engine.repository.TransactionLogRepository;
import io.grpc.StatusRuntimeException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class BillingService {

    private final BalanceServiceClient balanceClient;
    private final TransactionLogRepository txnRepo;

    /**
     * Process a single AdEvent.
     *
     * Returns a ProcessingResult that tells the consumer whether to:
     *   - ACK (SUCCESS or REJECTED — both are terminal, no retry needed)
     *   - RETRY (transient error — consumer will retry with backoff)
     *   - DLQ (max retries exceeded — route to dead-letter topic)
     */
    @Transactional
    public ProcessingResult process(AdEvent event) {
        log.info("Processing event: id={} advertiser={} cost={} retryCount={}",
                event.getEventId(), event.getAdvertiserId(),
                event.getCostMicros(), event.getRetryCount());

        try {
            DeductFundsResponse response = balanceClient.deductFunds(
                    event.getAdvertiserId(),
                    event.getCostMicros(),
                    event.getEventId()
            );

            if (response.getStatus() == DeductFundsStatus.SUCCESS) {
                persistTxn(event, "SUCCESS", null);
                log.info("SUCCESS: event={} balanceAfter={}", event.getEventId(), response.getBalanceAfter());
                return ProcessingResult.SUCCESS;

            } else if (response.getStatus() == DeductFundsStatus.ALREADY_PROCESSED) {
                // gRPC-layer idempotency hit — this is a duplicate, ack silently
                log.info("DUPLICATE (balance layer): event={}", event.getEventId());
                return ProcessingResult.SUCCESS;

            } else if (response.getStatus() == DeductFundsStatus.INSUFFICIENT_FUNDS) {
                persistTxn(event, "REJECTED", "INSUFFICIENT_FUNDS");
                log.warn("REJECTED: event={} advertiser={} insufficient funds",
                        event.getEventId(), event.getAdvertiserId());
                return ProcessingResult.REJECTED;

            } else {
                // ADVERTISER_NOT_FOUND or other terminal errors
                persistTxn(event, "REJECTED", response.getStatus().name());
                return ProcessingResult.REJECTED;
            }

        } catch (StatusRuntimeException e) {
            // Transient gRPC failure — signal retry
            log.warn("Transient gRPC error for event={}: {}", event.getEventId(), e.getStatus());
            return ProcessingResult.RETRY;

        } catch (DataIntegrityViolationException e) {
            // PostgreSQL UNIQUE constraint fired — duplicate event, safe to ack
            log.info("Duplicate event caught by DB constraint: {}", event.getEventId());
            return ProcessingResult.SUCCESS;
        }
    }

    private void persistTxn(AdEvent event, String status, String reason) {
        try {
            txnRepo.save(TransactionLog.builder()
                    .eventId(event.getEventId())
                    .advertiserId(event.getAdvertiserId())
                    .campaignId(event.getCampaignId())
                    .eventType(event.getEventType())
                    .costMicros(event.getCostMicros())
                    .status(status)
                    .rejectReason(reason)
                    .retryCount(event.getRetryCount())
                    .processedAt(Instant.now())
                    .build());
        } catch (DataIntegrityViolationException e) {
            // Race condition: another thread already inserted this event_id — safe to ignore
            log.debug("Transaction log entry already exists for event_id={}", event.getEventId());
        }
    }

    public enum ProcessingResult {
        SUCCESS, REJECTED, RETRY
    }
}
