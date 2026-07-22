package com.fluxmesh.stream;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SlidingWindowTest {

    @Test
    void computesRollingStatistics() {
        SlidingWindow w = new SlidingWindow(60_000L);
        w.add(1000, 10.0);
        w.add(2000, 20.0);
        w.add(3000, 30.0);

        assertEquals(3, w.count());
        assertEquals(20.0, w.mean(), 1e-9);
        assertEquals(10.0, w.min(), 1e-9);
        assertEquals(30.0, w.max(), 1e-9);
        // population stdDev of {10,20,30} = sqrt(200/3)
        assertEquals(Math.sqrt(200.0 / 3.0), w.stdDev(), 1e-9);
    }

    @Test
    void evictsSamplesOlderThanTheWindow() {
        SlidingWindow w = new SlidingWindow(10_000L);
        w.add(1000, 100.0);
        w.add(2000, 100.0);

        // 20s later both originals are outside a 10s window
        w.add(20_000L, 50.0);

        assertEquals(1, w.count());
        assertEquals(50.0, w.mean(), 1e-9);
        assertEquals(50.0, w.min(), 1e-9);
        assertEquals(50.0, w.max(), 1e-9);
    }

    @Test
    void minAndMaxRecoverAfterTheExtremeIsEvicted() {
        SlidingWindow w = new SlidingWindow(10_000L);
        w.add(1000, 999.0);   // the outlier
        w.add(2000, 10.0);
        w.add(3000, 20.0);
        assertEquals(999.0, w.max(), 1e-9);

        // push past the outlier's lifetime
        w.add(12_500L, 30.0);

        assertTrue(w.max() < 999.0, "max must fall once the outlier is evicted, was " + w.max());
        assertEquals(30.0, w.max(), 1e-9);
        assertEquals(10.0, w.min(), 1e-9);
    }

    @Test
    void stdDevIsZeroForAConstantStream() {
        SlidingWindow w = new SlidingWindow(60_000L);
        for (int i = 1; i <= 10; i++) {
            w.add(i * 1000L, 42.0);
        }
        assertEquals(0.0, w.stdDev(), 1e-9);
        assertEquals(42.0, w.mean(), 1e-9);
    }

    @Test
    void emptyWindowIsSafeToQuery() {
        SlidingWindow w = new SlidingWindow(60_000L);
        assertTrue(w.isEmpty());
        assertEquals(0, w.count());
        assertEquals(0.0, w.mean());
        assertEquals(0.0, w.stdDev());
        assertEquals(0.0, w.min());
        assertEquals(0.0, w.max());
    }

    @Test
    void incrementalStatsStayAccurateOverManyEvictions() {
        SlidingWindow w = new SlidingWindow(5_000L);
        // stream well past the window so eviction runs constantly; the
        // incremental sums must not drift from a fresh computation
        for (int i = 1; i <= 5_000; i++) {
            w.add(i * 100L, (i % 7) * 1.0);
        }
        double mean = w.mean();
        double stdDev = w.stdDev();

        assertTrue(mean >= 0.0 && mean <= 6.0, "mean drifted to " + mean);
        assertTrue(stdDev >= 0.0 && stdDev <= 6.0, "stdDev drifted to " + stdDev);
        assertTrue(w.count() <= 51, "window should hold ~5s of 100ms samples");
    }

    @Test
    void tracksLastSeenAndLastValue() {
        SlidingWindow w = new SlidingWindow(60_000L);
        w.add(1000, 10.0);
        w.add(7000, 77.0);

        assertEquals(7000L, w.lastSeenTs());
        assertEquals(77.0, w.lastValue(), 1e-9);
    }
}
