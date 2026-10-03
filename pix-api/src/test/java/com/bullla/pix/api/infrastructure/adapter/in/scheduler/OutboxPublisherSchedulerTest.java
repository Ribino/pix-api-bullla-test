package com.bullla.pix.api.infrastructure.adapter.in.scheduler;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bullla.pix.api.application.usecase.outbox.PublishOutboxEventsUseCase;
import com.bullla.pix.api.application.usecase.outbox.PublishOutboxResult;
import com.bullla.pix.api.infrastructure.metrics.PixApiMetrics;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OutboxPublisherSchedulerTest {

    @Mock
    private PublishOutboxEventsUseCase publishOutboxEventsUseCase;

    @Mock
    private PixApiMetrics metrics;

    @Test
    void delegatesEachPollToTheUseCase() {
        when(publishOutboxEventsUseCase.execute()).thenReturn(new PublishOutboxResult(0, 0, 0));
        OutboxPublisherScheduler scheduler = new OutboxPublisherScheduler(publishOutboxEventsUseCase, metrics);

        scheduler.poll();

        verify(publishOutboxEventsUseCase).execute();
    }
}
