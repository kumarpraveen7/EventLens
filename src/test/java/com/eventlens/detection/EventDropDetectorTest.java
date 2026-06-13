package com.eventlens.detection;

import com.eventlens.config.EventLensProperties;
import com.eventlens.model.DropClassification;
import com.eventlens.model.EventChannel;
import com.eventlens.model.EventFinding;
import com.eventlens.model.EventSource;
import com.eventlens.model.EventStage;
import com.eventlens.model.GameEventRequest;
import com.eventlens.model.GameEventType;
import com.eventlens.model.ParticipantRole;
import com.eventlens.model.StreamAnalysis;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class EventDropDetectorTest {
    private final Instant now = Instant.parse("2026-06-13T05:00:00Z");
    private final EventDropDetector detector = new EventDropDetector(
            new EventLensProperties(1000, 50),
            new SuggestionCatalog(),
            Clock.fixed(now, ZoneOffset.UTC)
    );

    @Test
    void detectsMissingSequenceGap() {
        detector.ingest(event("evt-1", 1, now));

        List<EventFinding> findings = detector.ingest(event("evt-3", 3, now));

        assertThat(findings).hasSize(1);
        assertThat(findings.get(0).classification()).isEqualTo(DropClassification.SEQUENCE_GAP);
        assertThat(findings.get(0).missingSequences()).containsExactly(2L);
    }

    @Test
    void detectsDuplicateEventIdAndSequence() {
        detector.ingest(event("evt-1", 1, now));

        List<EventFinding> findings = detector.ingest(event("evt-1", 1, now));

        assertThat(findings)
                .extracting(EventFinding::classification)
                .containsExactly(DropClassification.DUPLICATE_EVENT, DropClassification.DUPLICATE_EVENT);
    }

    @Test
    void detectsLateEvent() {
        List<EventFinding> findings = detector.ingest(event("evt-1", 1, now.minusSeconds(5)));

        assertThat(findings)
                .extracting(EventFinding::classification)
                .contains(DropClassification.LATE_EVENT);
    }

    @Test
    void buildsStreamAnalysis() {
        detector.ingest(event("evt-1", 1, now));
        detector.ingest(event("evt-4", 4, now));

        StreamAnalysis analysis = detector.analysisFor("room-42:player-a");

        assertThat(analysis.lastSequence()).isEqualTo(4);
        assertThat(analysis.missingEventCount()).isEqualTo(2);
        assertThat(analysis.recentFindings()).hasSize(1);
    }

    @Test
    void classifiesLikelyNetworkDropFromLifecycleTelemetry() {
        List<EventFinding> findings = detector.ingest(event("evt-1", GameEventType.ANSWER_SUBMITTED, 1, now, EventStage.CLIENT_SENT));

        assertThat(findings)
                .extracting(EventFinding::classification)
                .contains(DropClassification.NETWORK_OR_GATEWAY_DROP);
    }

    @Test
    void classifiesEventBridgeToSqsDrop() {
        List<EventFinding> findings = detector.ingest(event("evt-1", GameEventType.BUZZ_IN, 1, now, EventStage.EVENTBRIDGE_RECEIVED));

        assertThat(findings)
                .extracting(EventFinding::classification)
                .contains(DropClassification.EVENTBRIDGE_TO_SQS_DROP);
    }

    @Test
    void detectsGuestParticipationBypass() {
        List<EventFinding> findings = detector.ingest(event(
                "evt-1",
                GameEventType.BUZZ_IN,
                1,
                now,
                EventStage.SERVER_RECEIVED,
                ParticipantRole.GUEST,
                "guest-1",
                "turn-1",
                "GAMEPLAY"
        ));

        assertThat(findings)
                .extracting(EventFinding::classification)
                .contains(DropClassification.GUEST_PARTICIPATION_BYPASS);
    }

    @Test
    void detectsStalePreviousPlayerResult() {
        detector.ingest(event("turn-started", GameEventType.TURN_STARTED, 1, now, EventStage.SERVER_PROCESSED));

        List<EventFinding> findings = detector.ingest(event(
                "result-old",
                GameEventType.ANSWER_VALIDATED,
                2,
                now,
                EventStage.SERVER_PROCESSED,
                ParticipantRole.PARTICIPANT,
                "player-b",
                "turn-old",
                "GAMEPLAY"
        ));

        assertThat(findings)
                .extracting(EventFinding::classification)
                .contains(DropClassification.STALE_TURN_RESULT);
    }

    @Test
    void detectsStaleScreenAfterResume() {
        detector.ingest(event("game-ended", GameEventType.GAME_ENDED, 1, now, EventStage.SERVER_PROCESSED));

        List<EventFinding> findings = detector.ingest(event(
                "resume",
                GameEventType.APP_FOREGROUNDED,
                2,
                now,
                EventStage.SERVER_PROCESSED,
                ParticipantRole.PARTICIPANT,
                "player-a",
                "turn-1",
                "GAMEPLAY"
        ));

        assertThat(findings)
                .extracting(EventFinding::classification)
                .contains(DropClassification.STALE_CLIENT_SCREEN);
    }

    private GameEventRequest event(String eventId, long sequence, Instant occurredAt) {
        return event(eventId, GameEventType.ANSWER_SUBMITTED, sequence, occurredAt, EventStage.SERVER_PROCESSED);
    }

    private GameEventRequest event(String eventId, GameEventType type, long sequence, Instant occurredAt, EventStage stage) {
        return event(eventId, type, sequence, occurredAt, stage, ParticipantRole.PARTICIPANT, "player-a", "turn-1", "GAMEPLAY");
    }

    private GameEventRequest event(
            String eventId,
            GameEventType type,
            long sequence,
            Instant occurredAt,
            EventStage stage,
            ParticipantRole role,
            String participantId,
            String turnId,
            String screen
    ) {
        return new GameEventRequest(
                "room-42:player-a",
                "room-42",
                participantId,
                type,
                stage == EventStage.EVENTBRIDGE_RECEIVED ? EventSource.IVS_EVENTBRIDGE : EventSource.BACKEND_MODERATOR,
                stage == EventStage.EVENTBRIDGE_RECEIVED ? EventChannel.EVENTBRIDGE : EventChannel.GAME_BACKEND,
                role,
                stage,
                eventId,
                sequence,
                "game-42",
                participantId,
                turnId,
                1,
                screen,
                occurredAt,
                Map.of("answer", "sample")
        );
    }
}
