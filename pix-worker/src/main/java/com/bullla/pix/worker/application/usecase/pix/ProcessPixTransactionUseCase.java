package com.bullla.pix.worker.application.usecase.pix;

import java.time.Instant;

import com.bullla.pix.worker.application.exception.UnknownPixTransactionException;
import com.bullla.pix.worker.domain.gateway.pix.PartnerResult;
import com.bullla.pix.worker.domain.gateway.pix.PixPartnerGateway;
import com.bullla.pix.worker.domain.gateway.pix.TransientPartnerException;
import com.bullla.pix.worker.domain.model.pix.PixTransaction;
import com.bullla.pix.worker.domain.model.pix.PixTransactionStatus;
import com.bullla.pix.worker.domain.repository.pix.PixTransactionRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ProcessPixTransactionUseCase {

    private static final Logger log = LoggerFactory.getLogger(ProcessPixTransactionUseCase.class);

    private final PixTransactionRepository pixTransactionRepository;
    private final PixPartnerGateway pixPartnerGateway;

    public ProcessPixTransactionUseCase(
            PixTransactionRepository pixTransactionRepository,
            PixPartnerGateway pixPartnerGateway) {
        this.pixTransactionRepository = pixTransactionRepository;
        this.pixPartnerGateway = pixPartnerGateway;
    }

    @Transactional
    public ProcessOutcome execute(PixRequestedEvent event) {
        PixTransaction transaction = pixTransactionRepository.findByTransactionId(event.transactionId())
                .orElseThrow(() -> new UnknownPixTransactionException(event.transactionId()));

        if (transaction.status() != PixTransactionStatus.PROCESSING) {
            log.info("skipping already terminal transaction transactionId={} status={}",
                    transaction.transactionId(), transaction.status());
            return ProcessOutcome.skipped();
        }

        try {
            PartnerResult partnerResult = pixPartnerGateway.process(transaction);
            return switch (partnerResult) {
                case APPROVED -> complete(transaction, PixTransactionStatus.SUCCESS);
                case DECLINED -> complete(transaction, PixTransactionStatus.FAILED);
            };
        } catch (TransientPartnerException exception) {
            log.warn("transient partner failure, eligible for async retry transactionId={} reason={}",
                    transaction.transactionId(), exception.getReason());
            return ProcessOutcome.retryable(exception.getReason());
        }
    }

    private ProcessOutcome complete(PixTransaction transaction, PixTransactionStatus status) {
        transaction.transitionTo(status, Instant.now());
        boolean transitioned = pixTransactionRepository.updateStatusIfProcessing(
                transaction.transactionId(), status, Instant.now());
        if (!transitioned) {
            log.info("transaction concurrently transitioned transactionId={}", transaction.transactionId());
            return ProcessOutcome.skipped();
        }
        log.info("transaction transitioned transactionId={} status={}", transaction.transactionId(), status);
        return ProcessOutcome.processed(status);
    }
}
