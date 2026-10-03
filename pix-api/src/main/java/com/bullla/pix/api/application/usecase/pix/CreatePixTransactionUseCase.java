package com.bullla.pix.api.application.usecase.pix;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import com.bullla.pix.api.application.exception.DuplicateTransactionConflictException;
import com.bullla.pix.api.domain.model.outbox.OutboxEvent;
import com.bullla.pix.api.domain.model.outbox.PixRequestedPayload;
import com.bullla.pix.api.domain.model.pix.PixTransaction;
import com.bullla.pix.api.domain.repository.outbox.OutboxEventRepository;
import com.bullla.pix.api.domain.repository.pix.PixTransactionRepository;
import com.bullla.pix.api.domain.service.RequestFingerprint;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class CreatePixTransactionUseCase {

    private final PixTransactionRepository pixTransactionRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final RequestFingerprint requestFingerprint;

    public CreatePixTransactionUseCase(
            PixTransactionRepository pixTransactionRepository,
            OutboxEventRepository outboxEventRepository,
            RequestFingerprint requestFingerprint) {
        this.pixTransactionRepository = pixTransactionRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.requestFingerprint = requestFingerprint;
    }

    @Transactional
    public PixTransactionResult execute(CreatePixTransactionCommand command) {
        String fingerprint = requestFingerprint.generate(
                command.transactionId(), command.amount(), command.pixKey(), command.description());

        Optional<PixTransaction> existing = pixTransactionRepository.findByTransactionId(command.transactionId());
        if (existing.isPresent()) {
            return resolveExisting(existing.get(), fingerprint);
        }

        Instant now = Instant.now();
        PixTransaction transaction = PixTransaction.processing(
                UUID.randomUUID(),
                command.transactionId(),
                command.amount(),
                command.pixKey(),
                command.description(),
                fingerprint,
                now);

        boolean inserted = pixTransactionRepository.insertIfAbsent(transaction);
        if (!inserted) {
            PixTransaction winner = pixTransactionRepository.findByTransactionId(command.transactionId())
                    .orElseThrow(() -> new IllegalStateException(
                            "Transaction " + command.transactionId() + " was concurrently created but could not be loaded"));
            return resolveExisting(winner, fingerprint);
        }

        outboxEventRepository.save(createOutboxEvent(transaction, now));

        return PixTransactionResult.created(transaction);
    }

    private PixTransactionResult resolveExisting(PixTransaction existing, String fingerprint) {
        if (!existing.requestFingerprint().equals(fingerprint)) {
            throw new DuplicateTransactionConflictException(existing.transactionId());
        }
        return PixTransactionResult.existing(existing);
    }

    private OutboxEvent createOutboxEvent(PixTransaction transaction, Instant now) {
        UUID eventId = UUID.randomUUID();
        PixRequestedPayload payload = new PixRequestedPayload(
                eventId,
                OutboxEvent.PIX_REQUESTED,
                transaction.transactionId(),
                transaction.amount(),
                transaction.pixKey(),
                transaction.description(),
                now);
        return OutboxEvent.pendingPixRequested(eventId, transaction.transactionId(), payload, now);
    }
}
