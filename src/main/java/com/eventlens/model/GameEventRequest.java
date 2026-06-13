package com.eventlens.model;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.util.Map;

public record GameEventRequest(
        @NotBlank String streamId,
        @NotBlank String roomId,
        @NotBlank String producerId,
        @NotNull GameEventType type,
        EventSource source,
        EventChannel channel,
        ParticipantRole role,
        @NotNull EventStage stage,
        @NotBlank String eventId,
        @Min(1) long sequence,
        String gameId,
        String participantId,
        String turnId,
        Integer roundNumber,
        String screen,
        @NotNull Instant occurredAt,
        Map<String, Object> payload
) {
}
