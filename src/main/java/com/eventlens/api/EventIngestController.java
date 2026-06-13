package com.eventlens.api;

import com.eventlens.detection.EventDropDetector;
import com.eventlens.model.EventFinding;
import com.eventlens.model.GameEventRequest;
import com.eventlens.model.IngestResponse;
import com.eventlens.model.StreamAnalysis;
import jakarta.validation.Valid;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@Validated
@RestController
@RequestMapping("/api")
public class EventIngestController {
    private final EventDropDetector detector;

    public EventIngestController(EventDropDetector detector) {
        this.detector = detector;
    }

    @PostMapping("/events")
    public IngestResponse ingest(@Valid @RequestBody GameEventRequest request) {
        List<EventFinding> findings = detector.ingest(request);
        return new IngestResponse(request.streamId(), 1, findings);
    }

    @PostMapping("/events/batch")
    public IngestResponse ingestBatch(@Valid @RequestBody List<@Valid GameEventRequest> requests) {
        List<EventFinding> findings = new ArrayList<>();
        String streamId = requests.isEmpty() ? "unknown" : requests.get(0).streamId();
        for (GameEventRequest request : requests) {
            findings.addAll(detector.ingest(request));
        }
        return new IngestResponse(streamId, requests.size(), findings);
    }

    @GetMapping("/streams")
    public List<StreamAnalysis> streams() {
        return detector.allAnalyses();
    }

    @GetMapping("/streams/{streamId}/analysis")
    public StreamAnalysis streamAnalysis(@PathVariable String streamId) {
        return detector.analysisFor(streamId);
    }
}
