package com.bullla.pix.api.application.usecase.pix;

import com.bullla.pix.api.application.exception.TransactionNotFoundException;
import com.bullla.pix.api.domain.model.pix.PixTransaction;
import com.bullla.pix.api.domain.repository.pix.PixTransactionRepository;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class GetPixTransactionUseCase {

    private final PixTransactionRepository pixTransactionRepository;

    public GetPixTransactionUseCase(PixTransactionRepository pixTransactionRepository) {
        this.pixTransactionRepository = pixTransactionRepository;
    }

    @Transactional(readOnly = true)
    public PixTransaction execute(String transactionId) {
        return pixTransactionRepository.findByTransactionId(transactionId)
                .orElseThrow(() -> new TransactionNotFoundException(transactionId));
    }
}
