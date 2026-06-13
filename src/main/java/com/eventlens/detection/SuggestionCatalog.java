package com.eventlens.detection;

import com.eventlens.model.DropClassification;
import org.springframework.stereotype.Component;

@Component
public class SuggestionCatalog {
    public String suggestionFor(DropClassification classification) {
        return switch (classification) {
            case SEQUENCE_GAP -> "Persist events before publish, send them through an ordered queue, and replay missing sequence numbers after reconnect or ACK timeout.";
            case DUPLICATE_EVENT -> "Make consumers idempotent by eventId and sequence, then ACK duplicates without applying score/state changes twice.";
            case OUT_OF_ORDER_EVENT -> "Buffer a small sequence window per game room and apply events only when the next expected sequence arrives.";
            case LATE_EVENT -> "Track client clock skew and network latency; move critical game actions to ACK-based delivery with bounded retries.";
            case CLIENT_DISPATCH_DROP -> "On Android, persist the event before emitting it, drain events from a single WorkManager/coroutine queue, and mark sent only after the transport accepts it.";
            case EVENTBRIDGE_TO_SQS_DROP -> "Enable EventBridge retry policy, route failed invocations to a DLQ, and alarm when the SQS enqueue count is lower than matched IVS events.";
            case LAMBDA_THROTTLE_OR_SQS_BACKLOG -> "Increase Lambda reserved concurrency, tune SQS visibility timeout, and alarm on ApproximateAgeOfOldestMessage before gameplay SLAs are breached.";
            case LAMBDA_CONSUMER_FAILURE -> "Make the Lambda consumer idempotent, persist the raw event before processing, and send failed records to a DLQ with replay tooling.";
            case DLQ_RECOVERY_REQUIRED -> "Replay the DLQ record after verifying idempotency keys, then trigger a game-state resync for affected participants.";
            case NETWORK_OR_GATEWAY_DROP -> "Add server-side receive ACKs, retry unacked sends with exponential backoff, and reconnect using the last acknowledged sequence.";
            case SERVER_PROCESSING_DROP -> "Wrap game-state mutations in an idempotent transaction, publish processing failures to a dead-letter queue, and alert on handler exceptions.";
            case ACK_DELIVERY_DROP -> "Treat ACKs as retryable messages; expose an ACK status endpoint so the client can reconcile after reconnect.";
            case MISSING_BACKEND_RESPONSE -> "Put turn evaluation behind a tracked command/result pair, timeout unresolved commands, and broadcast an authoritative resync instead of leaving clients waiting.";
            case STALE_TURN_RESULT -> "Attach turnId, roundNumber, and participantId to every score/result event; reject results that do not match the backend moderator's active turn.";
            case MODERATOR_STATE_DRIFT -> "Make backend moderator state authoritative and force mobile moderators to reconcile from a versioned state snapshot after backgrounding or reconnect.";
            case STALE_CLIENT_SCREEN -> "On app resume, fetch the current room snapshot and navigate from the backend phase instead of restoring the old Activity/Fragment blindly.";
            case ROUND_DIALOG_OVERLAP -> "Represent leaderboard display as a server-timed phase and send a single phase transition when the next round can start.";
            case AUDIO_LEAK_AFTER_GAME_END -> "Tie IVS publish/subscribe teardown to the GAME_ENDED phase and confirm audio stopped before navigating users away.";
            case CHAT_ORDER_DRIFT -> "Split game-control events from chat messages or add per-channel ordering keys so comment traffic cannot reorder gameplay events.";
            case GUEST_PARTICIPATION_BYPASS -> "Gate participant-only commands at the backend using token role and onboarding state, not only Android navigation flow.";
            case HEALTHY -> "No action needed. Keep monitoring sequence, latency, and ACK metrics.";
        };
    }
}
