# EventLens

EventLens is a Spring Boot service for finding dropped events in event-driven games.
The first use case is inspired by a real-time Antakshari flow built around Amazon IVS Stage,
IVS chat/custom events, EventBridge, SQS, Lambda, and a backend moderator that is the source
of truth for Android and iOS clients.

## What it detects

- Sequence gaps: expected event `n`, received event `n + k`.
- Duplicate events: same `eventId` or same sequence arrives again.
- Out-of-order events: older sequence arrives after a newer one.
- Late events: event occurred too long before the service received it.
- Lifecycle drops: the event disappears between Android queue, network, server handler, ACK, and client ACK.
- AWS handoff drops: IVS/EventBridge event does not reach SQS/Lambda/backend, or lands in DLQ.
- Gameplay anomalies: stale turn results, missing backend responses, stale resumed screens, round dialog overlap, guest participation bypass, audio leaks, and chat ordering drift.

## Why this matters

In a real-time game, one dropped event can create visible bugs:

- Player answer submitted but server never scores it.
- Timer tick or round-end event is missed.
- Score update applies twice after retry.
- Reconnect resumes from the wrong game state.
- Local Android/iOS moderator keeps showing lobby/gameplay after backend already moved phase.
- Chat comments and game-control events share a channel and arrive in confusing order.

EventLens turns those symptoms into concrete classifications and remediation ideas.

See [docs/antakshari-event-model.md](docs/antakshari-event-model.md) for the full event model and scenario mapping.

## API

Start the app:

```bash
mvn spring-boot:run
```

Ingest one event:

```bash
curl -X POST http://localhost:8080/api/events \
  -H 'Content-Type: application/json' \
  -d '{
    "streamId": "room-42:player-a",
    "roomId": "room-42",
    "producerId": "player-a",
    "type": "ANSWER_SUBMITTED",
    "source": "BACKEND_MODERATOR",
    "channel": "GAME_BACKEND",
    "role": "PARTICIPANT",
    "stage": "SERVER_PROCESSED",
    "eventId": "evt-1",
    "sequence": 1,
    "gameId": "game-42",
    "participantId": "player-a",
    "turnId": "turn-1",
    "roundNumber": 1,
    "screen": "GAMEPLAY",
    "occurredAt": "2026-06-13T05:00:00Z",
    "payload": {
      "answer": "Mere Sapno Ki Rani"
    }
  }'
```

Ingest a batch:

```bash
curl -X POST http://localhost:8080/api/events/batch \
  -H 'Content-Type: application/json' \
  -d '[
    {
      "streamId": "room-42:player-a",
      "roomId": "room-42",
      "producerId": "player-a",
      "type": "ANSWER_SUBMITTED",
      "source": "BACKEND_MODERATOR",
      "channel": "GAME_BACKEND",
      "role": "PARTICIPANT",
      "stage": "SERVER_PROCESSED",
      "eventId": "evt-1",
      "sequence": 1,
      "gameId": "game-42",
      "participantId": "player-a",
      "turnId": "turn-1",
      "roundNumber": 1,
      "screen": "GAMEPLAY",
      "occurredAt": "2026-06-13T05:00:00Z",
      "payload": {"answer": "A"}
    },
    {
      "streamId": "room-42:player-a",
      "roomId": "room-42",
      "producerId": "player-a",
      "type": "SCORE_UPDATED",
      "source": "BACKEND_MODERATOR",
      "channel": "GAME_BACKEND",
      "role": "PARTICIPANT",
      "stage": "SERVER_PROCESSED",
      "eventId": "evt-3",
      "sequence": 3,
      "gameId": "game-42",
      "participantId": "player-a",
      "turnId": "turn-1",
      "roundNumber": 1,
      "screen": "GAMEPLAY",
      "occurredAt": "2026-06-13T05:00:01Z",
      "payload": {"score": 10}
    }
  ]'
```

Inspect a stream:

```bash
curl http://localhost:8080/api/streams/room-42:player-a/analysis
```

## Example finding

```json
{
  "classification": "SEQUENCE_GAP",
  "severity": "CRITICAL",
  "streamId": "room-42:player-a",
  "eventId": "evt-3",
  "sequence": 3,
  "missingSequences": [2],
  "reason": "Expected sequence 2 but received 3.",
  "suggestedFix": "Persist events before publish, send them through an ordered queue, and replay missing sequence numbers after reconnect or ACK timeout.",
  "detectedAt": "2026-06-13T05:00:02Z"
}
```

## Lifecycle stages

Send the same `eventId` as it moves through the system:

- `CLIENT_CREATED`
- `CLIENT_QUEUED`
- `CLIENT_SENT`
- `EVENTBRIDGE_RECEIVED`
- `SQS_ENQUEUED`
- `LAMBDA_STARTED`
- `LAMBDA_COMPLETED`
- `SERVER_RECEIVED`
- `SERVER_PROCESSED`
- `ACK_SENT`
- `CLIENT_ACKED`
- `DLQ_WRITTEN`

EventLens uses missing stage transitions to classify likely causes such as Android dispatch drops,
EventBridge-to-SQS drops, Lambda throttling, server processing drops, and ACK delivery drops.

## Antakshari scenarios covered

| Symptom | EventLens classification |
| --- | --- |
| Player sings, but no result dialog arrives | `MISSING_BACKEND_RESPONSE` |
| Player 2 is singing, but server sends Player 1 result | `STALE_TURN_RESULT` |
| Guest bypasses onboarding and tries to play | `GUEST_PARTICIPATION_BYPASS` |
| App resumes into lobby after backend moved to gameplay | `STALE_CLIENT_SCREEN` |
| Round starts while leaderboard dialog is still open | `ROUND_DIALOG_OVERLAP` |
| Audio is still heard after game end | `AUDIO_LEAK_AFTER_GAME_END` |
| Chat messages are not ordered | `CHAT_ORDER_DRIFT` |
| IVS/EventBridge event never reaches SQS | `EVENTBRIDGE_TO_SQS_DROP` |
| SQS event waits too long for Lambda | `LAMBDA_THROTTLE_OR_SQS_BACKLOG` |
| Failed event lands in DLQ | `DLQ_RECOVERY_REQUIRED` |

## Next project steps

- Add ACK timeout detection for critical events.
- Add a simulated IVS/EventBridge/SQS/Lambda producer that randomly drops events.
- Store events and findings in Postgres.
- Move stream state from memory to Redis for multi-instance deployment.
- Publish Micrometer metrics for dashboards.
- Add WebSocket/SSE live room monitoring.
