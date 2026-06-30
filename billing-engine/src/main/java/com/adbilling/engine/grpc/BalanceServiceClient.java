package com.adbilling.engine.grpc;

import com.adbilling.balance.grpc.*;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Component;

/**
 * gRPC client wrapper for AccountBalanceService.
 * Why gRPC over REST for this internal call?
 *   - Typed contract via .proto — breaking changes are compile-time errors
 *   - Binary serialization (protobuf) is ~5-10x smaller than JSON
 *   - ~30% lower latency vs HTTP/1.1 JSON on high-frequency internal paths
 *   - Streaming support if we ever need to batch balance updates
 */
@Slf4j
@Component
public class BalanceServiceClient {

    @GrpcClient("balance-service")
    private AccountBalanceServiceGrpc.AccountBalanceServiceBlockingStub stub;

    public DeductFundsResponse deductFunds(String advertiserId, long amountMicros, String eventId) {
        try {
            DeductFundsRequest request = DeductFundsRequest.newBuilder()
                    .setAdvertiserId(advertiserId)
                    .setAmountMicros(amountMicros)
                    .setIdempotencyKey(eventId)
                    .build();
            return stub.deductFunds(request);
        } catch (StatusRuntimeException e) {
            log.error("gRPC DeductFunds failed: advertiserId={} eventId={} status={}",
                    advertiserId, eventId, e.getStatus());
            throw e;
        }
    }

    public CheckBalanceResponse checkBalance(String advertiserId) {
        return stub.checkBalance(
                CheckBalanceRequest.newBuilder().setAdvertiserId(advertiserId).build());
    }
}
