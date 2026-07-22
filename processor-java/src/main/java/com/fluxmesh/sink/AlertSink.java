package com.fluxmesh.sink;

import com.fluxmesh.model.Alert;

import java.util.List;

/**
 * Where fired alerts go.
 *
 * <p>The in-memory ring is the default; MongoDB is activated by the
 * {@code mongo} profile. Alerts are append-only and queried newest-first, which
 * is why a capped ring and a Mongo collection are interchangeable here.
 */
public interface AlertSink {

    void write(Alert alert);

    /** Newest first, optionally filtered. */
    List<Alert> recent(String deviceId, String severity, int limit);

    long count();

    String name();
}
