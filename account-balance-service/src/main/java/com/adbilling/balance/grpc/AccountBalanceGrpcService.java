package com.adbilling.balance.grpc;

import com.adbilling.balance.service.BalanceService;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * gRPC server implementation that delegates to BalanceService.
 * The @GrpcService annotation registers this with grpc-server-spring-boot-starter.
 */
@Slf4j
@GrpcService
@RequiredArgsConstructor
public class AccountBalanceGrpcService extends AccountBalanceServiceGrpc.AccountBalanceServiceImplBase {

    private final BalanceService balanceService;

    @Override
    public void checkBalance(CheckBalanceRequest request,
                             StreamObserver<CheckBalanceResponse> responseObserver) {
        Long balance = balanceService.getBalance(request.getAdvertiserId());
        long balanceMicros = (balance != null) ? balance : 0L;

        responseObserver.onNext(CheckBalanceResponse.newBuilder()
                .setAdvertiserId(request.getAdvertiserId())
                .setBalanceMicros(balanceMicros)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void deductFunds(DeductFundsRequest request,
                            StreamObserver<DeductFundsResponse> responseObserver) {
        long result = balanceService.deductFunds(
                request.getAdvertiserId(),
                request.getAmountMicros(),
                request.getIdempotencyKey()
        );

        DeductFundsResponse.Builder resp = DeductFundsResponse.newBuilder();

        if (result == -2) {
            resp.setStatus(DeductFundsStatus.ALREADY_PROCESSED)
                .setMessage("Idempotency key already processed: " + request.getIdempotencyKey());
        } else if (result == -1) {
            resp.setStatus(DeductFundsStatus.ADVERTISER_NOT_FOUND)
                .setMessage("Advertiser not found: " + request.getAdvertiserId());
        } else if (result == 0) {
            resp.setStatus(DeductFundsStatus.INSUFFICIENT_FUNDS)
                .setMessage("Insufficient balance for advertiser: " + request.getAdvertiserId());
        } else {
            resp.setStatus(DeductFundsStatus.SUCCESS)
                .setBalanceAfter(result)
                .setMessage("Deduction successful");
        }

        responseObserver.onNext(resp.build());
        responseObserver.onCompleted();
    }

    @Override
    public void seedBalance(SeedBalanceRequest request,
                            StreamObserver<SeedBalanceResponse> responseObserver) {
        try {
            balanceService.seedBalance(request.getAdvertiserId(), request.getBalanceMicros());
            responseObserver.onNext(SeedBalanceResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Balance seeded")
                    .build());
        } catch (Exception e) {
            log.error("Failed to seed balance for {}", request.getAdvertiserId(), e);
            responseObserver.onNext(SeedBalanceResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage(e.getMessage())
                    .build());
        }
        responseObserver.onCompleted();
    }
}
