package com.fluxmesh.model;

import com.fluxmesh.cep.PatternDetector;

/** A fired complex-event pattern, as served over GraphQL and written to the sink. */
public record Alert(
        String alertId,
        String deviceId,
        String site,
        String metric,
        String pattern,
        String severity,
        double triggerValue,
        double threshold,
        int consecutiveCount,
        double windowMean,
        long ts) {

    public static Alert from(Reading reading, PatternDetector.Match match, double windowMean) {
        return new Alert(
                reading.deviceId() + ":" + match.pattern() + ":" + match.ts(),
                reading.deviceId(),
                reading.site(),
                reading.metric(),
                match.pattern().name(),
                match.severity().name(),
                round2(match.triggerValue()),
                round2(match.threshold()),
                match.consecutiveCount(),
                round2(windowMean),
                match.ts());
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
