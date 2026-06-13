package com.eventlens.model;

import java.util.List;

public record StreamAnalysis(
        String streamId,
        long lastSequence,
        long totalEventsSeen,
        long duplicateEvents,
        long outOfOrderEvents,
        long lateEvents,
        long lifecycleDropSuspicions,
        long gameplayAnomalies,
        long missingEventCount,
        List<EventFinding> recentFindings
) {
}
