package com.eventlens.model;

import java.time.Instant;
import java.util.List;

public record EventFinding(
        DropClassification classification,
        EventSeverity severity,
        String streamId,
        String eventId,
        long sequence,
        List<Long> missingSequences,
        String reason,
        String suggestedFix,
        Instant detectedAt
) {
}
