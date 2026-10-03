package com.bullla.pix.worker.infrastructure.adapter.in.messaging;

import com.bullla.pix.worker.application.exception.UnknownPixTransactionException;
import com.bullla.pix.worker.application.usecase.pix.PixRequestedEvent;
import com.bullla.pix.worker.application.usecase.pix.PixRetryRouter;
import com.bullla.pix.worker.application.usecase.pix.ProcessOutcome;
import com.bullla.pix.worker.application.usecase.pix.ProcessPixTransactionUseCase;
import com.bullla.pix.worker.domain.model.pix.PixTransactionStatus;
import com.bullla.pix.worker.infrastructure.metrics.PixWorkerMetrics;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Timer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class PixRequestedListener {

    private static final Logger log = LoggerFactory.getLogger(PixRequestedListener.class);

    private final ProcessPixTransactionUseCase processPixTransactionUseCase;
    private final PixRetryRouter pixRetryRouter;
    private final ObjectMapper objectMapper;
    private final PixWorkerMetrics metrics;

    public PixRequestedListener(
            ProcessPixTransactionUseCase processPixTransactionUseCase,
            PixRetryRouter pixRetryRouter,
            ObjectMapper objectMapper,
            PixWorkerMetrics metrics) {
        this.processPixTransactionUseCase = processPixTransactionUseCase;
        this.pixRetryRouter = pixRetryRouter;
        this.objectMapper = objectMapper;
        this.metrics = metrics;
    }

    @KafkaListener(
            topics = "${pix.kafka.topic:pix.requested}",
            groupId = "${pix.kafka.group-id:pix-processing}")
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        PixRequestedEvent event;
        try {
            event = objectMapper.readValue(record.value(), PixRequestedEvent.class);
        } catch (Exception exception) {
            log.error("dropping malformed event topic={} partition={} offset={} error={}",
                    record.topic(), record.partition(), record.offset(), exception.toString());
            acknowledgment.acknowledge();
            return;
        }
        if (event.transactionId() == null || event.transactionId().isBlank()) {
            log.error("dropping event without transactionId topic={} partition={} offset={} eventId={}",
                    record.topic(), record.partition(), record.offset(), event.eventId());
            acknowledgment.acknowledge();
            return;
        }
        RetryHeaders.Parsed headers = RetryHeaders.parse(record.headers());
        log.info("transactionId={} event=pix_processing_started topic={} partition={} offset={}",
                event.transactionId(), record.topic(), record.partition(), record.offset());
        Timer.Sample sample = metrics.startProcessing();
        try {
            ProcessOutcome outcome = processPixTransactionUseCase.execute(event);
            log.info("transactionId={} event=pix_processing_finished result={}",
                    event.transactionId(), outcome.status());
            switch (outcome.status()) {
                case PROCESSED -> {
                    String result = outcome.transactionStatus() == PixTransactionStatus.SUCCESS ? "success" : "failed";
                    metrics.countProcessing(result);
                    metrics.stopProcessing(sample, result);
                    acknowledgment.acknowledge();
                }
                case SKIPPED -> acknowledgment.acknowledge();
                case RETRYABLE -> {
                    boolean settled = pixRetryRouter.route(
                            event.transactionId(), record.value(), headers.retryCount(),
                            outcome.reason(), headers.firstFailureAt(), record.topic());
                    if (settled) {
                        acknowledgment.acknowledge();
                    }
                }
            }
        } catch (UnknownPixTransactionException exception) {
            log.error("dropping event for unknown transaction topic={} partition={} offset={} transactionId={}",
                    record.topic(), record.partition(), record.offset(), exception.getTransactionId());
            acknowledgment.acknowledge();
        } catch (Exception exception) {
            log.warn("processing failed, message will be redelivered topic={} partition={} offset={} transactionId={} error={}",
                    record.topic(), record.partition(), record.offset(), event.transactionId(), exception.toString());
        }
    }
}
