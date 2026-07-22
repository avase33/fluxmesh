package com.fluxmesh.stream;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * A time-based sliding window over one device's readings for one metric.
 *
 * <p>This is the piece Flink would give you as keyed state plus a sliding
 * window assigner. Written by hand here for two reasons: it runs with no
 * cluster, and it makes the actual mechanics visible — Flink's value is
 * distribution and fault tolerance, not the arithmetic.
 *
 * <p>Rolling {@code sum} and {@code sumSquares} are updated incrementally on
 * every insert and eviction, so {@code count / mean / stdDev} are O(1) per
 * event regardless of how many readings the window holds. {@code min} and
 * {@code max} are the exception: they need a scan after the extreme value is
 * evicted, which is why they are computed lazily rather than tracked eagerly.
 *
 * <p>Not thread-safe on its own — {@code KeyedState} guards each window.
 */
public final class SlidingWindow {

    private record Sample(long ts, double value) {
    }

    private final Deque<Sample> samples = new ArrayDeque<>();
    private final long windowMillis;
    private double sum;
    private double sumSquares;

    public SlidingWindow(long windowMillis) {
        this.windowMillis = windowMillis;
    }

    /** Adds a reading and evicts anything older than the window. */
    public void add(long ts, double value) {
        sum += value;
        sumSquares += value * value;
        samples.addLast(new Sample(ts, value));
        evictOlderThan(ts);
    }

    private void evictOlderThan(long now) {
        while (!samples.isEmpty() && now - samples.peekFirst().ts() > windowMillis) {
            Sample old = samples.removeFirst();
            sum -= old.value();
            sumSquares -= old.value() * old.value();
        }
    }

    public int count() {
        return samples.size();
    }

    public boolean isEmpty() {
        return samples.isEmpty();
    }

    public double mean() {
        return samples.isEmpty() ? 0.0 : sum / samples.size();
    }

    /** Population standard deviation, clamped to absorb float cancellation. */
    public double stdDev() {
        if (samples.isEmpty()) {
            return 0.0;
        }
        double mean = mean();
        double variance = Math.max(sumSquares / samples.size() - mean * mean, 0.0);
        return Math.sqrt(variance);
    }

    public double min() {
        double min = Double.POSITIVE_INFINITY;
        for (Sample s : samples) {
            min = Math.min(min, s.value());
        }
        return samples.isEmpty() ? 0.0 : min;
    }

    public double max() {
        double max = Double.NEGATIVE_INFINITY;
        for (Sample s : samples) {
            max = Math.max(max, s.value());
        }
        return samples.isEmpty() ? 0.0 : max;
    }

    public long lastSeenTs() {
        return samples.isEmpty() ? 0L : samples.peekLast().ts();
    }

    public double lastValue() {
        return samples.isEmpty() ? 0.0 : samples.peekLast().value();
    }

    public long windowMillis() {
        return windowMillis;
    }
}
