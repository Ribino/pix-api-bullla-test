package com.bullla.pix.api.application.usecase.outbox;

public record PublishOutboxResult(int claimed, int published, int failed) {
}
