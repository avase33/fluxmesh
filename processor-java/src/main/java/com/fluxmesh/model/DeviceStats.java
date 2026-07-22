package com.fluxmesh.model;

/** Window statistics for one device/metric pair. */
public record DeviceStats(
        String deviceId,
        String site,
        String metric,
        int count,
        double mean,
        double min,
        double max,
        double stdDev,
        long windowMillis,
        long lastSeenTs,
        boolean stale) {
}
