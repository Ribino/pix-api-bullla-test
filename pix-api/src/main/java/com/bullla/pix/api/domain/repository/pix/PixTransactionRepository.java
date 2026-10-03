package com.bullla.pix.api.domain.repository.pix;

import java.util.Optional;

import com.bullla.pix.api.domain.model.pix.PixTransaction;

public interface PixTransactionRepository {

    boolean insertIfAbsent(PixTransaction transaction);

    Optional<PixTransaction> findByTransactionId(String transactionId);
}
