package com.fluxmesh.cep;

import com.fluxmesh.stream.SlidingWindow;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PatternDetectorTest {

    private PatternDetector detector;
    private SlidingWindow window;

    @BeforeEach
    void setUp() {
        detector = new PatternDetector();
        window = new SlidingWindow(60_000L);
    }

    /** Feeds a reading through the window first, mirroring the real pipeline. */
    private List<PatternDetector.Match> feed(long ts, double value) {
        window.add(ts, value);
        return detector.onReading(ts, value, window);
    }

    @Test
    void singleBreachDoesNotFire() {
        // one reading over the line is noise, not a fault
        List<PatternDetector.Match> matches = feed(1000, 105.0);
        assertTrue(matches.stream().noneMatch(m -> m.pattern() == PatternDetector.Pattern.OVERHEAT));
    }

    @Test
    void threeConsecutiveBreachesFireOverheat() {
        assertTrue(feed(1000, 101.0).isEmpty());
        assertTrue(feed(2000, 102.0).isEmpty());

        List<PatternDetector.Match> matches = feed(3000, 103.0);

        PatternDetector.Match overheat = matches.stream()
                .filter(m -> m.pattern() == PatternDetector.Pattern.OVERHEAT)
                .findFirst().orElse(null);
        assertNotNull(overheat, "third consecutive breach should fire OVERHEAT");
        assertEquals(PatternDetector.Severity.CRITICAL, overheat.severity());
        assertEquals(3, overheat.consecutiveCount());
        assertEquals(103.0, overheat.triggerValue());
    }

    @Test
    void runResetsWhenValueDropsBackUnderThreshold() {
        feed(1000, 101.0);
        feed(2000, 102.0);
        feed(3000, 95.0);   // back to normal: the run must reset
        assertEquals(0, detector.overheatRun());

        assertTrue(feed(4000, 101.0).isEmpty());
        assertTrue(feed(5000, 101.5).isEmpty());
        // only now, on the third of the NEW run, should it fire
        assertTrue(feed(6000, 102.0).stream()
                .anyMatch(m -> m.pattern() == PatternDetector.Pattern.OVERHEAT));
    }

    @Test
    void sustainedFaultFiresOnceNotRepeatedly() {
        for (int i = 1; i <= 3; i++) {
            feed(i * 1000L, 101.0 + i);
        }
        // already fired on the third; further breaches must stay quiet
        int laterFirings = 0;
        for (int i = 4; i <= 10; i++) {
            laterFirings += (int) feed(i * 1000L, 101.0 + i).stream()
                    .filter(m -> m.pattern() == PatternDetector.Pattern.OVERHEAT)
                    .count();
        }
        assertEquals(0, laterFirings, "a sustained fault should not spam alerts");
    }

    @Test
    void flatlineDetectsAStuckSensor() {
        List<PatternDetector.Match> fired = List.of();
        for (int i = 1; i <= 11; i++) {
            fired = feed(i * 1000L, 42.0);
        }
        assertTrue(fired.stream().anyMatch(m -> m.pattern() == PatternDetector.Pattern.FLATLINE)
                        || detectedFlatlineEarlier(),
                "an unchanging value should eventually trip FLATLINE");
    }

    private boolean detectedFlatlineEarlier() {
        // the pattern fires exactly on the Nth repeat; re-run to locate it
        PatternDetector fresh = new PatternDetector();
        SlidingWindow w = new SlidingWindow(60_000L);
        for (int i = 1; i <= 11; i++) {
            w.add(i * 1000L, 42.0);
            if (fresh.onReading(i * 1000L, 42.0, w).stream()
                    .anyMatch(m -> m.pattern() == PatternDetector.Pattern.FLATLINE)) {
                return true;
            }
        }
        return false;
    }

    @Test
    void changingValuesNeverTripFlatline() {
        for (int i = 1; i <= 20; i++) {
            List<PatternDetector.Match> matches = feed(i * 1000L, 40.0 + i * 0.5);
            assertTrue(matches.stream().noneMatch(m -> m.pattern() == PatternDetector.Pattern.FLATLINE));
        }
    }

    @Test
    void spikeNeedsHistoryBeforeItCanFire() {
        // with only a couple of samples stdDev is meaningless; must not fire
        assertTrue(feed(1000, 50.0).isEmpty());
        assertTrue(feed(2000, 900.0).stream()
                .noneMatch(m -> m.pattern() == PatternDetector.Pattern.SPIKE));
    }

    @Test
    void spikeFiresOnAJumpOutsideNormalVariation() {
        for (int i = 1; i <= 10; i++) {
            feed(i * 1000L, 50.0 + (i % 2));   // tight band around 50
        }
        List<PatternDetector.Match> matches = feed(11_000L, 200.0);

        assertTrue(matches.stream().anyMatch(m -> m.pattern() == PatternDetector.Pattern.SPIKE),
                "a large jump outside the window's variability should fire SPIKE");
    }

    @Test
    void flatWindowDoesNotProduceInfiniteSpikes() {
        // a perfectly constant window has stdDev 0; dividing by it would make
        // every reading infinitely many deviations from the mean
        for (int i = 1; i <= 10; i++) {
            feed(i * 1000L, 50.0);
        }
        List<PatternDetector.Match> matches = feed(11_000L, 50.0);
        assertTrue(matches.stream().noneMatch(m -> m.pattern() == PatternDetector.Pattern.SPIKE));
    }

    @Test
    void dropoutFiresOnlyAfterTheTimeout() {
        feed(1000, 50.0);

        assertNull(detector.checkDropout(20_000L), "20s of silence is within tolerance");

        PatternDetector.Match dropout = detector.checkDropout(40_000L);
        assertNotNull(dropout, "30s+ of silence should fire DROPOUT");
        assertEquals(PatternDetector.Pattern.DROPOUT, dropout.pattern());
        assertEquals(PatternDetector.Severity.CRITICAL, dropout.severity());
    }

    @Test
    void dropoutStaysSilentForADeviceThatNeverReported() {
        assertNull(new PatternDetector().checkDropout(999_999L),
                "a device with no history is not a dropout");
    }

    @Test
    void overheatThresholdIsConfigurable() {
        PatternDetector strict = new PatternDetector(
                new PatternDetector.Config(50.0, 2, 4.0, 10, 30_000L));
        SlidingWindow w = new SlidingWindow(60_000L);

        w.add(1000, 60.0);
        assertTrue(strict.onReading(1000, 60.0, w).isEmpty());
        w.add(2000, 61.0);
        assertTrue(strict.onReading(2000, 61.0, w).stream()
                .anyMatch(m -> m.pattern() == PatternDetector.Pattern.OVERHEAT),
                "custom config should fire on the 2nd breach at threshold 50");
    }

    @Test
    void exactlyAtThresholdIsNotABreach() {
        // ">" not ">=" — a reading exactly at the limit is in spec
        for (int i = 1; i <= 5; i++) {
            assertFalse(feed(i * 1000L, 100.0).stream()
                    .anyMatch(m -> m.pattern() == PatternDetector.Pattern.OVERHEAT));
        }
    }
}
