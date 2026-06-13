package com.eventlens.model;

import java.util.List;

public record IngestResponse(
        String streamId,
        long acceptedEvents,
        List<EventFinding> findings
) {
}
