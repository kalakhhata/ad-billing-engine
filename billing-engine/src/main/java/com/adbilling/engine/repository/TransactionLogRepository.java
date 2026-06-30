package com.adbilling.engine.repository;

import com.adbilling.engine.model.TransactionLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface TransactionLogRepository extends JpaRepository<TransactionLog, Long> {

    Optional<TransactionLog> findByEventId(String eventId);

    @Query("SELECT COALESCE(SUM(t.costMicros), 0) FROM TransactionLog t " +
           "WHERE t.advertiserId = :advertiserId AND t.status = 'SUCCESS'")
    long sumSuccessfulSpend(@Param("advertiserId") String advertiserId);
}
