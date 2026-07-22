package com.fluxmesh.cep;

import com.fluxmesh.stream.SlidingWindow;

import java.util.ArrayList;
import java.util.List;

/**
 * Complex event processing over a single device/metric stream.
 *
 * <p>The state each pattern needs is small and explicit: a run length for
 * consecutive breaches, the previous value for flatline detection. That is
 * exactly what Flink's {@code CEP} library manages for you across a cluster;
 * doing it by hand here keeps the semantics inspectable.
 *
 * <p>The design decision worth defending: {@code OVERHEAT} requires
 * <b>N consecutive</b> breaches rather than firing on the first reading over
 * the line. One sample above a threshold is usually sensor noise. Requiring a
 * run trades a few hundred milliseconds of detection latency for a drastic
 * drop in false positives — and an alerting system people ignore is worse than
 * no alerting system.
 */
public final class PatternDetector {

    public enum Pattern {
        OVERHEAT, SPIKE, FLATLINE, DROPOUT
    }

    public enum Severity {
        INFO, WARNING, CRITICAL
    }

    /** A fired pattern. Immutable; the caller decides what to do with it. */
    public record Match(
            Pattern pattern,
            Severity severity,
            double triggerValue,
            double threshold,
            int consecutiveCount,
            long ts) {
    }

    public record Config(
            double overheatThreshold,
            int overheatConsecutive,
            double spikeStdDevs,
            int flatlineConsecutive,
            long dropoutMillis) {

        public static Config defaults() {
            return new Config(100.0, 3, 4.0, 10, 30_000L);
        }
    }

    private final Config config;

    // --- per-stream CEP state -------------------------------------------
    private int overheatRun;
    private int flatlineRun;
    private Double previousValue;
    private long lastTs;

    public PatternDetector(Config config) {
        this.config = config;
    }

    public PatternDetector() {
        this(Config.defaults());
    }

    /**
     * Feeds one reading and returns every pattern it completes.
     *
     * @param window the device's sliding window, already updated with this reading
     */
    public List<Match> onReading(long ts, double value, SlidingWindow window) {
        List<Match> matches = new ArrayList<>(2);

        // --- OVERHEAT: N consecutive readings above the threshold --------
        if (value > config.overheatThreshold()) {
            overheatRun++;
            if (overheatRun == config.overheatConsecutive()) {
                // fire once on the transition, not on every subsequent reading,
                // so a sustained fault does not spam the operator
                matches.add(new Match(Pattern.OVERHEAT, Severity.CRITICAL,
                        value, config.overheatThreshold(), overheatRun, ts));
            }
        } else {
            overheatRun = 0;
        }

        // --- FLATLINE: an unchanging value means a stuck sensor ----------
        if (previousValue != null && Double.compare(previousValue, value) == 0) {
            flatlineRun++;
            if (flatlineRun == config.flatlineConsecutive()) {
                matches.add(new Match(Pattern.FLATLINE, Severity.WARNING,
                        value, value, flatlineRun, ts));
            }
        } else {
            flatlineRun = 0;
        }

        // --- SPIKE: a jump far outside the window's own variability ------
        // Needs enough history for stdDev to mean anything, and a non-zero
        // stdDev — a perfectly flat window would make every reading infinitely
        // many deviations away.
        double stdDev = window.stdDev();
        if (window.count() >= 5 && stdDev > 1e-9) {
            double deviations = (value - window.mean()) / stdDev;
            if (deviations >= config.spikeStdDevs()) {
                matches.add(new Match(Pattern.SPIKE, Severity.WARNING,
                        value, window.mean() + config.spikeStdDevs() * stdDev,
                        1, ts));
            }
        }

        previousValue = value;
        lastTs = ts;
        return matches;
    }

    /**
     * Checks for a silent device. Called on a timer rather than on a reading,
     * because the defining feature of this pattern is that nothing arrived.
     */
    public Match checkDropout(long now) {
        if (lastTs > 0 && now - lastTs > config.dropoutMillis()) {
            return new Match(Pattern.DROPOUT, Severity.CRITICAL,
                    0.0, config.dropoutMillis(), 0, now);
        }
        return null;
    }

    public long lastTs() {
        return lastTs;
    }

    public int overheatRun() {
        return overheatRun;
    }

    public Config config() {
        return config;
    }
}
