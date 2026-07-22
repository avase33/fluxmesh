package com.fluxmesh.stream;

import com.fluxmesh.cep.PatternDetector;
import com.fluxmesh.model.DeviceStats;
import com.fluxmesh.model.Reading;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeyedStateTest {

    private KeyedState state;

    @BeforeEach
    void setUp() {
        state = new KeyedState(60_000L, PatternDetector.Config.defaults());
    }

    private Reading reading(String device, String metric, double value, long ts) {
        return new Reading(device, "plant-a", metric, value, ts);
    }

    @Test
    void separatesStateByDeviceAndMetric() {
        state.apply(reading("motor-1", "temperature_c", 50.0, 1000));
        state.apply(reading("motor-2", "temperature_c", 90.0, 1000));
        state.apply(reading("motor-1", "vibration", 3.0, 1000));

        assertEquals(3, state.keyCount(), "device+metric pairs are separate partitions");
        assertEquals(50.0, state.stats("motor-1", "temperature_c").orElseThrow().mean(), 1e-9);
        assertEquals(90.0, state.stats("motor-2", "temperature_c").orElseThrow().mean(), 1e-9);
        assertEquals(3.0, state.stats("motor-1", "vibration").orElseThrow().mean(), 1e-9);
    }

    @Test
    void oneDevicesFaultDoesNotAffectAnother() {
        // motor-1 overheats; motor-2 stays healthy the whole time
        for (int i = 1; i <= 5; i++) {
            state.apply(reading("motor-1", "temperature_c", 100.0 + i, i * 1000L));
            state.apply(reading("motor-2", "temperature_c", 70.0, i * 1000L));
        }
        assertTrue(state.stats("motor-1", "temperature_c").orElseThrow().mean() > 100.0);
        assertEquals(70.0, state.stats("motor-2", "temperature_c").orElseThrow().mean(), 1e-9);
    }

    @Test
    void matchesAreReturnedForTheBreachingDeviceOnly() {
        state.apply(reading("motor-1", "temperature_c", 101.0, 1000));
        state.apply(reading("motor-1", "temperature_c", 102.0, 2000));
        KeyedState.Processed hot = state.apply(reading("motor-1", "temperature_c", 103.0, 3000));

        KeyedState.Processed cool = state.apply(reading("motor-2", "temperature_c", 70.0, 3000));

        assertTrue(hot.matches().stream()
                .anyMatch(m -> m.pattern() == PatternDetector.Pattern.OVERHEAT));
        assertTrue(cool.matches().isEmpty());
    }

    @Test
    void unknownDeviceHasNoStats() {
        assertTrue(state.stats("nobody", "temperature_c").isEmpty());
    }

    @Test
    void listsDevicesAndFiltersBySite() {
        state.apply(new Reading("a", "plant-a", "temperature_c", 10.0, 1000));
        state.apply(new Reading("b", "plant-b", "temperature_c", 20.0, 1000));

        assertEquals(2, state.allStats(null).size());
        assertEquals(1, state.allStats("plant-a").size());
        assertEquals("a", state.allStats("plant-a").get(0).deviceId());
    }

    @Test
    void marksADeviceStaleAfterTheDropoutTimeout() {
        // timestamped far in the past relative to now
        state.apply(new Reading("old", "plant-a", "temperature_c", 10.0,
                System.currentTimeMillis() - 120_000L));
        DeviceStats stats = state.stats("old", "temperature_c").orElseThrow();
        assertTrue(stats.stale(), "a device silent for 2 minutes should read as stale");
    }

    @Test
    void freshDeviceIsNotStale() {
        state.apply(new Reading("fresh", "plant-a", "temperature_c", 10.0,
                System.currentTimeMillis()));
        assertFalse(state.stats("fresh", "temperature_c").orElseThrow().stale());
    }

    @Test
    void sweepFindsSilentDevices() {
        long past = System.currentTimeMillis() - 120_000L;
        state.apply(new Reading("silent", "plant-a", "temperature_c", 10.0, past));

        List<KeyedState.DropoutEvent> events = state.sweepDropouts(System.currentTimeMillis());

        assertEquals(1, events.size());
        assertEquals("silent", events.get(0).deviceId());
        assertEquals(PatternDetector.Pattern.DROPOUT, events.get(0).match().pattern());
    }

    @Test
    void concurrentUpdatesToDifferentDevicesDoNotCorruptState() throws Exception {
        int devices = 8;
        int perDevice = 500;
        ExecutorService pool = Executors.newFixedThreadPool(devices);
        CountDownLatch done = new CountDownLatch(devices);

        for (int d = 0; d < devices; d++) {
            final String id = "motor-" + d;
            pool.submit(() -> {
                try {
                    for (int i = 1; i <= perDevice; i++) {
                        state.apply(reading(id, "temperature_c", 50.0, i * 100L));
                    }
                } finally {
                    done.countDown();
                }
            });
        }

        assertTrue(done.await(30, TimeUnit.SECONDS), "workers should finish");
        pool.shutdownNow();

        assertEquals(devices, state.keyCount());
        for (int d = 0; d < devices; d++) {
            DeviceStats stats = state.stats("motor-" + d, "temperature_c").orElseThrow();
            // every sample was 50.0, so mean must be exactly 50 and stdDev 0
            assertEquals(50.0, stats.mean(), 1e-9, "device " + d + " mean drifted");
            assertEquals(0.0, stats.stdDev(), 1e-9, "device " + d + " stdDev drifted");
        }
    }
}
