package com.bullla.pix.worker.domain.repository.pix;

import java.time.Instant;
import java.util.Optional;

import com.bullla.pix.worker.domain.model.pix.PixTransaction;
import com.bullla.pix.worker.domain.model.pix.PixTransactionStatus;

public interface PixTransactionRepository {

    Optional<PixTransaction> findByTransactionId(String transactionId);

    boolean updateStatusIfProcessing(String transactionId, PixTransactionStatus status, Instant updatedAt);
}
