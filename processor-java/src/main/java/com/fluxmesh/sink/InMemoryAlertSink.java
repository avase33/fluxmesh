package com.fluxmesh.sink;

import com.fluxmesh.model.Alert;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bounded in-memory alert history — the offline default.
 *
 * <p>Capped deliberately: telemetry alerting runs forever, so an unbounded list
 * is a memory leak with extra steps. The oldest alert is dropped once the cap
 * is reached, which is the right trade-off for a live operations view. The
 * {@code mongo} profile is what you switch to when you need real retention.
 */
@Component
@Profile("!mongo")
public class InMemoryAlertSink implements AlertSink {

    private final Deque<Alert> alerts = new ArrayDeque<>();
    private final int capacity;
    private final AtomicLong total = new AtomicLong();

    public InMemoryAlertSink(@Value("${fluxmesh.sink.capacity:2000}") int capacity) {
        this.capacity = Math.max(capacity, 1);
    }

    @Override
    public synchronized void write(Alert alert) {
        if (alerts.size() >= capacity) {
            alerts.removeLast();
        }
        alerts.addFirst(alert);   // newest first
        total.incrementAndGet();
    }

    @Override
    public synchronized List<Alert> recent(String deviceId, String severity, int limit) {
        List<Alert> out = new ArrayList<>(Math.min(limit, alerts.size()));
        for (Alert a : alerts) {
            if (deviceId != null && !deviceId.isBlank() && !deviceId.equals(a.deviceId())) {
                continue;
            }
            if (severity != null && !severity.isBlank() && !severity.equalsIgnoreCase(a.severity())) {
                continue;
            }
            out.add(a);
            if (out.size() >= limit) {
                break;
            }
        }
        return out;
    }

    @Override
    public long count() {
        return total.get();
    }

    @Override
    public String name() {
        return "memory";
    }
}
