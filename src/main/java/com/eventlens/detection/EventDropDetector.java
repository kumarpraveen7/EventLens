package com.eventlens.detection;

import com.eventlens.config.EventLensProperties;
import com.eventlens.model.DropClassification;
import com.eventlens.model.EventChannel;
import com.eventlens.model.EventFinding;
import com.eventlens.model.EventSeverity;
import com.eventlens.model.EventSource;
import com.eventlens.model.EventStage;
import com.eventlens.model.GameEventRequest;
import com.eventlens.model.GameEventType;
import com.eventlens.model.ParticipantRole;
import com.eventlens.model.StreamAnalysis;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class EventDropDetector {
    private final EventLensProperties properties;
    private final SuggestionCatalog suggestions;
    private final Clock clock;
    private final Map<String, StreamState> streams = new ConcurrentHashMap<>();

    @Autowired
    public EventDropDetector(EventLensProperties properties, SuggestionCatalog suggestions) {
        this(properties, suggestions, Clock.systemUTC());
    }

    EventDropDetector(EventLensProperties properties, SuggestionCatalog suggestions, Clock clock) {
        this.properties = properties;
        this.suggestions = suggestions;
        this.clock = clock;
    }

    public List<EventFinding> ingest(GameEventRequest event) {
        StreamState state = streams.computeIfAbsent(event.streamId(), ignored -> new StreamState(properties.maxEventsPerStream()));
        synchronized (state) {
            return state.record(event, Instant.now(clock), properties.lateEventThresholdMs(), suggestions);
        }
    }

    public StreamAnalysis analysisFor(String streamId) {
        StreamState state = streams.get(streamId);
        if (state == null) {
            return new StreamAnalysis(streamId, 0, 0, 0, 0, 0, 0, 0, 0, List.of());
        }

        synchronized (state) {
            return state.analysis(streamId);
        }
    }

    public List<StreamAnalysis> allAnalyses() {
        return streams.keySet().stream()
                .sorted()
                .map(this::analysisFor)
                .toList();
    }

    private static final class StreamState {
        private final int maxFindings;
        private final Set<String> eventIds = new HashSet<>();
        private final Set<Long> seenSequences = new HashSet<>();
        private final Map<String, EnumSet<EventStage>> eventStages = new ConcurrentHashMap<>();
        private final Map<String, PendingEvaluation> pendingEvaluations = new HashMap<>();
        private final Deque<EventFinding> recentFindings = new ArrayDeque<>();
        private long lastSequence;
        private long lastChatSequence;
        private long totalEventsSeen;
        private long duplicateEvents;
        private long outOfOrderEvents;
        private long lateEvents;
        private long lifecycleDropSuspicions;
        private long gameplayAnomalies;
        private long missingEventCount;
        private String activeTurnId;
        private String activeParticipantId;
        private Integer activeRound;
        private String canonicalScreen = "LOBBY";
        private boolean roundLeaderboardOpen;
        private boolean gameEnded;

        private StreamState(int maxFindings) {
            this.maxFindings = Math.max(1, maxFindings);
        }

        private List<EventFinding> record(
                GameEventRequest event,
                Instant receivedAt,
                long lateThresholdMs,
                SuggestionCatalog suggestions
        ) {
            totalEventsSeen++;
            List<EventFinding> findings = new ArrayList<>();

            if (!eventIds.add(event.eventId())) {
                EnumSet<EventStage> stages = eventStages.computeIfAbsent(event.eventId(), ignored -> EnumSet.noneOf(EventStage.class));
                if (stages.contains(event.stage())) {
                    duplicateEvents++;
                    findings.add(finding(event, receivedAt, DropClassification.DUPLICATE_EVENT, EventSeverity.WARNING,
                            Collections.emptyList(), "EventId and lifecycle stage were already seen for this stream.", suggestions));
                }
            }
            eventStages.computeIfAbsent(event.eventId(), ignored -> EnumSet.noneOf(EventStage.class)).add(event.stage());

            if (event.stage() == EventStage.SERVER_PROCESSED) {
                if (!seenSequences.add(event.sequence())) {
                    duplicateEvents++;
                    findings.add(finding(event, receivedAt, DropClassification.DUPLICATE_EVENT, EventSeverity.WARNING,
                            Collections.emptyList(), "Sequence number was already applied for this stream.", suggestions));
                } else if (event.sequence() < lastSequence) {
                    outOfOrderEvents++;
                    findings.add(finding(event, receivedAt, DropClassification.OUT_OF_ORDER_EVENT, EventSeverity.WARNING,
                            Collections.emptyList(), "Sequence arrived after a newer event was already seen.", suggestions));
                } else if (lastSequence > 0 && event.sequence() > lastSequence + 1) {
                    List<Long> missing = missingSequences(lastSequence + 1, event.sequence() - 1);
                    missingEventCount += missing.size();
                    findings.add(finding(event, receivedAt, DropClassification.SEQUENCE_GAP, EventSeverity.CRITICAL,
                            missing, "Expected sequence " + (lastSequence + 1) + " but received " + event.sequence() + ".", suggestions));
                }
            }

            long eventAgeMs = Duration.between(event.occurredAt(), receivedAt).toMillis();
            if (eventAgeMs > lateThresholdMs) {
                lateEvents++;
                findings.add(finding(event, receivedAt, DropClassification.LATE_EVENT, EventSeverity.WARNING,
                        Collections.emptyList(), "Event arrived " + eventAgeMs + " ms after it occurred.", suggestions));
            }

            if (event.stage() == EventStage.SERVER_PROCESSED && event.sequence() > lastSequence) {
                lastSequence = event.sequence();
            }

            lifecycleFinding(event, receivedAt, suggestions).ifPresent(findings::add);
            findings.addAll(gameplayFindings(event, receivedAt, suggestions));
            findings.forEach(this::remember);
            return findings;
        }

        private StreamAnalysis analysis(String streamId) {
            return new StreamAnalysis(
                    streamId,
                    lastSequence,
                    totalEventsSeen,
                    duplicateEvents,
                    outOfOrderEvents,
                    lateEvents,
                    lifecycleDropSuspicions,
                    gameplayAnomalies,
                    missingEventCount,
                    List.copyOf(recentFindings)
            );
        }

        private java.util.Optional<EventFinding> lifecycleFinding(
                GameEventRequest event,
                Instant receivedAt,
                SuggestionCatalog suggestions
        ) {
            EnumSet<EventStage> stages = eventStages.getOrDefault(event.eventId(), EnumSet.noneOf(EventStage.class));
            DropClassification classification = null;
            String reason = null;

            if (event.stage() == EventStage.CLIENT_QUEUED && !stages.contains(EventStage.CLIENT_SENT)) {
                classification = DropClassification.CLIENT_DISPATCH_DROP;
                reason = "Event reached the Android queue but has no CLIENT_SENT telemetry yet.";
            } else if (event.stage() == EventStage.EVENTBRIDGE_RECEIVED && !stages.contains(EventStage.SQS_ENQUEUED)) {
                classification = DropClassification.EVENTBRIDGE_TO_SQS_DROP;
                reason = "IVS/EventBridge matched the event but no SQS enqueue telemetry has arrived.";
            } else if (event.stage() == EventStage.SQS_ENQUEUED && !stages.contains(EventStage.LAMBDA_STARTED)) {
                classification = DropClassification.LAMBDA_THROTTLE_OR_SQS_BACKLOG;
                reason = "Event is in SQS but Lambda has not started processing it.";
            } else if (event.stage() == EventStage.LAMBDA_STARTED
                    && !stages.contains(EventStage.LAMBDA_COMPLETED)
                    && !stages.contains(EventStage.SERVER_RECEIVED)) {
                classification = DropClassification.LAMBDA_CONSUMER_FAILURE;
                reason = "Lambda started but there is no completion or backend receive telemetry.";
            } else if (event.stage() == EventStage.DLQ_WRITTEN) {
                classification = DropClassification.DLQ_RECOVERY_REQUIRED;
                reason = "Event was written to DLQ and needs replay or manual recovery.";
            } else if (event.stage() == EventStage.CLIENT_SENT && !stages.contains(EventStage.SERVER_RECEIVED)) {
                classification = DropClassification.NETWORK_OR_GATEWAY_DROP;
                reason = "Client marked the event sent but the server has not observed SERVER_RECEIVED.";
            } else if (event.stage() == EventStage.SERVER_RECEIVED && !stages.contains(EventStage.SERVER_PROCESSED)) {
                classification = DropClassification.SERVER_PROCESSING_DROP;
                reason = "Server received the event but no processing telemetry has arrived.";
            } else if (event.stage() == EventStage.ACK_SENT && !stages.contains(EventStage.CLIENT_ACKED)) {
                classification = DropClassification.ACK_DELIVERY_DROP;
                reason = "Server sent an ACK but the client has not observed it.";
            }

            if (classification == null) {
                return java.util.Optional.empty();
            }

            lifecycleDropSuspicions++;
            return java.util.Optional.of(finding(event, receivedAt, classification, EventSeverity.WARNING,
                    Collections.emptyList(), reason, suggestions));
        }

        private List<EventFinding> gameplayFindings(
                GameEventRequest event,
                Instant receivedAt,
                SuggestionCatalog suggestions
        ) {
            List<EventFinding> findings = new ArrayList<>();

            if (isGuestTryingToPlay(event)) {
                findings.add(finding(event, receivedAt, DropClassification.GUEST_PARTICIPATION_BYPASS, EventSeverity.CRITICAL,
                        Collections.emptyList(), "Guest/viewer token attempted a participant-only game action.", suggestions));
            }

            if (event.type() == GameEventType.RESULT_WAIT_TIMEOUT) {
                findings.add(finding(event, receivedAt, DropClassification.MISSING_BACKEND_RESPONSE, EventSeverity.CRITICAL,
                        Collections.emptyList(), "Client is still waiting for the backend result/evaluation event.", suggestions));
            }

            if ((event.type() == GameEventType.TURN_STARTED || event.type() == GameEventType.TURN_ASSIGNED)
                    && !pendingEvaluations.isEmpty()) {
                findings.add(finding(event, receivedAt, DropClassification.MISSING_BACKEND_RESPONSE, EventSeverity.CRITICAL,
                        Collections.emptyList(), "A new turn started while a previous answer evaluation was unresolved.", suggestions));
            }

            if (isResultEvent(event) && staleTurnResult(event)) {
                findings.add(finding(event, receivedAt, DropClassification.STALE_TURN_RESULT, EventSeverity.CRITICAL,
                        Collections.emptyList(), "Result does not match the backend moderator's active turn or participant.", suggestions));
            }

            if (event.type() == GameEventType.CLIENT_MODERATOR_SNAPSHOT && moderatorDrift(event)) {
                findings.add(finding(event, receivedAt, DropClassification.MODERATOR_STATE_DRIFT, EventSeverity.WARNING,
                        Collections.emptyList(), "Client-side moderator state differs from backend moderator state.", suggestions));
            }

            if ((event.type() == GameEventType.SCREEN_SNAPSHOT || event.type() == GameEventType.APP_FOREGROUNDED)
                    && screenDrift(event.screen())) {
                findings.add(finding(event, receivedAt, DropClassification.STALE_CLIENT_SCREEN, EventSeverity.WARNING,
                        Collections.emptyList(), "Client is showing " + event.screen() + " while backend phase is " + canonicalScreen + ".", suggestions));
            }

            if ((event.type() == GameEventType.AUDIO_STARTED || event.type() == GameEventType.PARTICIPANT_PUBLISHED)
                    && gameEnded) {
                findings.add(finding(event, receivedAt, DropClassification.AUDIO_LEAK_AFTER_GAME_END, EventSeverity.CRITICAL,
                        Collections.emptyList(), "Audio/media publication continued after backend marked the game ended.", suggestions));
            }

            if (event.type() == GameEventType.ROUND_STARTED && roundLeaderboardOpen) {
                findings.add(finding(event, receivedAt, DropClassification.ROUND_DIALOG_OVERLAP, EventSeverity.WARNING,
                        Collections.emptyList(), "Next round started while the local leaderboard phase was still open.", suggestions));
            }

            if (event.type() == GameEventType.CHAT_MESSAGE
                    && event.channel() == EventChannel.IVS_CHAT
                    && lastChatSequence > 0
                    && event.sequence() < lastChatSequence) {
                findings.add(finding(event, receivedAt, DropClassification.CHAT_ORDER_DRIFT, EventSeverity.INFO,
                        Collections.emptyList(), "IVS chat message arrived behind the latest observed chat sequence.", suggestions));
            }

            gameplayAnomalies += findings.size();
            updateGameplayState(event, receivedAt);
            return findings;
        }

        private boolean isGuestTryingToPlay(GameEventRequest event) {
            return event.role() == ParticipantRole.GUEST && switch (event.type()) {
                case BUZZ_IN, TRANSCRIPT_CHUNK, ANSWER_SUBMITTED, ANSWER_VALIDATED, SCORE_UPDATED, TURN_STARTED -> true;
                default -> false;
            };
        }

        private boolean isResultEvent(GameEventRequest event) {
            return event.type() == GameEventType.ANSWER_VALIDATED || event.type() == GameEventType.SCORE_UPDATED;
        }

        private boolean staleTurnResult(GameEventRequest event) {
            boolean turnMismatch = activeTurnId != null && event.turnId() != null && !activeTurnId.equals(event.turnId());
            boolean participantMismatch = activeParticipantId != null
                    && event.participantId() != null
                    && !activeParticipantId.equals(event.participantId());
            boolean roundMismatch = activeRound != null && event.roundNumber() != null && !activeRound.equals(event.roundNumber());
            return turnMismatch || participantMismatch || roundMismatch;
        }

        private boolean moderatorDrift(GameEventRequest event) {
            boolean clientSource = event.source() == EventSource.ANDROID_CLIENT || event.source() == EventSource.IOS_CLIENT;
            String clientTurnId = payloadString(event, "moderatorTurnId");
            String clientScreen = payloadString(event, "moderatorScreen");
            return clientSource
                    && ((activeTurnId != null && clientTurnId != null && !activeTurnId.equals(clientTurnId))
                    || screenDrift(clientScreen));
        }

        private boolean screenDrift(String screen) {
            return screen != null && !screen.isBlank() && canonicalScreen != null && !canonicalScreen.equalsIgnoreCase(screen);
        }

        private void updateGameplayState(GameEventRequest event, Instant receivedAt) {
            switch (event.type()) {
                case SESSION_CREATED, ROOM_CREATED, LOBBY_JOINED -> {
                    canonicalScreen = "LOBBY";
                    gameEnded = false;
                }
                case GAME_STARTED, ROUND_STARTED -> {
                    canonicalScreen = "GAMEPLAY";
                    roundLeaderboardOpen = false;
                    gameEnded = false;
                    activeRound = event.roundNumber();
                }
                case TURN_ASSIGNED, TURN_STARTED -> {
                    canonicalScreen = "GAMEPLAY";
                    roundLeaderboardOpen = false;
                    gameEnded = false;
                    activeTurnId = event.turnId();
                    activeParticipantId = event.participantId();
                    activeRound = event.roundNumber();
                }
                case ANSWER_SUBMITTED, TRANSCRIPT_CHUNK -> {
                    if (event.turnId() != null) {
                        pendingEvaluations.put(event.turnId(), new PendingEvaluation(event.eventId(), event.turnId(), receivedAt));
                    }
                }
                case ANSWER_VALIDATED, SCORE_UPDATED -> {
                    if (event.turnId() != null) {
                        pendingEvaluations.remove(event.turnId());
                    }
                }
                case ROUND_ENDED, ROUND_LEADERBOARD_SHOWN -> {
                    canonicalScreen = "ROUND_LEADERBOARD";
                    roundLeaderboardOpen = true;
                    activeRound = event.roundNumber();
                }
                case ROUND_LEADERBOARD_CLOSED -> {
                    canonicalScreen = "GAMEPLAY";
                    roundLeaderboardOpen = false;
                }
                case GAME_ENDED, SESSION_ENDED -> {
                    canonicalScreen = "ENDED";
                    gameEnded = true;
                    roundLeaderboardOpen = false;
                    activeTurnId = null;
                    activeParticipantId = null;
                    pendingEvaluations.clear();
                }
                case CHAT_MESSAGE -> lastChatSequence = Math.max(lastChatSequence, event.sequence());
                default -> {
                }
            }
        }

        private String payloadString(GameEventRequest event, String key) {
            if (event.payload() == null || !event.payload().containsKey(key)) {
                return null;
            }
            Object value = event.payload().get(key);
            return value == null ? null : String.valueOf(value);
        }

        private void remember(EventFinding finding) {
            recentFindings.addFirst(finding);
            while (recentFindings.size() > maxFindings) {
                recentFindings.removeLast();
            }
        }

        private EventFinding finding(
                GameEventRequest event,
                Instant detectedAt,
                DropClassification classification,
                EventSeverity severity,
                List<Long> missingSequences,
                String reason,
                SuggestionCatalog suggestions
        ) {
            return new EventFinding(
                    classification,
                    severity,
                    event.streamId(),
                    event.eventId(),
                    event.sequence(),
                    missingSequences,
                    reason,
                    suggestions.suggestionFor(classification),
                    detectedAt
            );
        }

        private List<Long> missingSequences(long startInclusive, long endInclusive) {
            List<Long> missing = new ArrayList<>();
            for (long sequence = startInclusive; sequence <= endInclusive; sequence++) {
                missing.add(sequence);
            }
            return missing;
        }

        private record PendingEvaluation(String eventId, String turnId, Instant submittedAt) {
        }
    }
}
