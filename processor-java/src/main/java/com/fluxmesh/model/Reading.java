package com.fluxmesh.model;

/**
 * One sensor sample (see {@code proto/protocol.md}).
 *
 * @param ts epoch milliseconds; ingest stamps arrival time when this is 0
 */
public record Reading(
        String deviceId,
        String site,
        String metric,
        double value,
        long ts) {

    /** Key used for per-device, per-metric state partitioning. */
    public String streamKey() {
        return deviceId + "|" + metric;
    }

    public Reading normalized(long nowMillis) {
        return new Reading(
                blank(deviceId) ? "unknown" : deviceId,
                blank(site) ? "default" : site,
                blank(metric) ? "value" : metric,
                value,
                ts > 0 ? ts : nowMillis);
    }

    private static boolean blank(String s) {
        return s == null || s.isBlank();
    }
}
