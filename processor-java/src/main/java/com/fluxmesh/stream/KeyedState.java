package com.fluxmesh.stream;

import com.fluxmesh.cep.PatternDetector;
import com.fluxmesh.model.DeviceStats;
import com.fluxmesh.model.Reading;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-key state for the stream: one sliding window and one CEP detector for
 * every {@code deviceId|metric} pair.
 *
 * <p>This is the concept Flink calls <b>keyed state</b>. Partitioning by key is
 * what makes the whole thing parallelisable: two different devices share
 * nothing, so they can be processed on different threads — or on different
 * machines — with no coordination. Only samples for the <i>same</i> key must be
 * ordered relative to each other.
 *
 * <p>Concretely: lookup is lock-free via {@link ConcurrentHashMap}, mutation is
 * guarded by the individual entry's monitor. Different devices never contend;
 * a single device's updates stay linearisable.
 */
public class KeyedState {

    /** State owned by one stream key. Guarded by its own monitor. */
    private static final class Entry {
        final SlidingWindow window;
        final PatternDetector detector;
        String site = "default";
        String deviceId;
        String metric;

        Entry(long windowMillis, PatternDetector.Config config) {
            this.window = new SlidingWindow(windowMillis);
            this.detector = new PatternDetector(config);
        }
    }

    private final Map<String, Entry> entries = new ConcurrentHashMap<>();
    private final long windowMillis;
    private final PatternDetector.Config config;

    public KeyedState(long windowMillis, PatternDetector.Config config) {
        this.windowMillis = windowMillis;
        this.config = config;
    }

    /** Result of feeding one reading through its key's state. */
    public record Processed(List<PatternDetector.Match> matches, double windowMean) {
    }

    /** Applies a reading to its key's window and detector. */
    public Processed apply(Reading reading) {
        Entry entry = entries.computeIfAbsent(reading.streamKey(),
                k -> new Entry(windowMillis, config));
        synchronized (entry) {
            entry.deviceId = reading.deviceId();
            entry.metric = reading.metric();
            entry.site = reading.site();
            entry.window.add(reading.ts(), reading.value());
            List<PatternDetector.Match> matches =
                    entry.detector.onReading(reading.ts(), reading.value(), entry.window);
            return new Processed(matches, entry.window.mean());
        }
    }

    /**
     * Sweeps every key for devices that have gone silent.
     *
     * <p>Runs on a timer rather than on arrival, because the defining feature of
     * a dropout is that nothing arrived to trigger anything.
     */
    public List<DropoutEvent> sweepDropouts(long now) {
        List<DropoutEvent> events = new ArrayList<>();
        for (Map.Entry<String, Entry> e : entries.entrySet()) {
            Entry entry = e.getValue();
            synchronized (entry) {
                PatternDetector.Match match = entry.detector.checkDropout(now);
                if (match != null) {
                    events.add(new DropoutEvent(entry.deviceId, entry.site, entry.metric, match));
                }
            }
        }
        return events;
    }

    public record DropoutEvent(String deviceId, String site, String metric,
                               PatternDetector.Match match) {
    }

    public Optional<DeviceStats> stats(String deviceId, String metric) {
        Entry entry = entries.get(deviceId + "|" + metric);
        if (entry == null) {
            return Optional.empty();
        }
        synchronized (entry) {
            return Optional.of(toStats(entry, System.currentTimeMillis()));
        }
    }

    public List<DeviceStats> allStats(String site) {
        long now = System.currentTimeMillis();
        List<DeviceStats> out = new ArrayList<>(entries.size());
        for (Entry entry : entries.values()) {
            synchronized (entry) {
                if (site == null || site.isBlank() || site.equals(entry.site)) {
                    out.add(toStats(entry, now));
                }
            }
        }
        out.sort((a, b) -> a.deviceId().compareTo(b.deviceId()));
        return out;
    }

    private DeviceStats toStats(Entry entry, long now) {
        boolean stale = entry.window.lastSeenTs() > 0
                && now - entry.window.lastSeenTs() > config.dropoutMillis();
        return new DeviceStats(
                entry.deviceId,
                entry.site,
                entry.metric,
                entry.window.count(),
                round2(entry.window.mean()),
                round2(entry.window.min()),
                round2(entry.window.max()),
                round2(entry.window.stdDev()),
                entry.window.windowMillis(),
                entry.window.lastSeenTs(),
                stale);
    }

    public int keyCount() {
        return entries.size();
    }

    public void clear() {
        entries.clear();
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
