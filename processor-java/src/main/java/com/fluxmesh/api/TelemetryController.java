package com.fluxmesh.api;

import com.fluxmesh.model.Alert;
import com.fluxmesh.model.DeviceStats;
import com.fluxmesh.sink.AlertSink;
import com.fluxmesh.stream.StreamProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;

import java.util.List;
import java.util.Map;

/** GraphQL resolvers backing {@code schema.graphqls}. */
@Controller
public class TelemetryController {

    private final StreamProcessor processor;
    private final AlertSink sink;
    private final long windowMillis;

    public TelemetryController(StreamProcessor processor, AlertSink sink,
                               @Value("${fluxmesh.window-millis:60000}") long windowMillis) {
        this.processor = processor;
        this.sink = sink;
        this.windowMillis = windowMillis;
    }

    @QueryMapping
    public List<DeviceStats> devices(@Argument String site) {
        return processor.state().allStats(site);
    }

    @QueryMapping
    public DeviceStats stats(@Argument String deviceId, @Argument String metric) {
        return processor.state().stats(deviceId, metric).orElse(null);
    }

    @QueryMapping
    public List<Alert> alerts(@Argument String deviceId, @Argument String severity,
                              @Argument Integer limit) {
        int max = limit == null ? 50 : Math.min(Math.max(limit, 1), 500);
        return sink.recent(deviceId, severity, max);
    }

    @QueryMapping
    public Map<String, Object> pipeline() {
        return Map.of(
                "processed", (double) processor.processed(),
                "alertsFired", (double) processor.alertsFired(),
                "readingsPerSecond", processor.readingsPerSecond(),
                "activeKeys", processor.state().keyCount(),
                "sink", sink.name(),
                "windowMillis", (double) windowMillis);
    }
}
