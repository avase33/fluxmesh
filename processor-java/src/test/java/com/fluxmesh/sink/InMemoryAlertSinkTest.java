package com.fluxmesh.sink;

import com.fluxmesh.model.Alert;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryAlertSinkTest {

    private static Alert alert(String device, String severity, long ts) {
        return new Alert(device + ":" + ts, device, "plant-a", "temperature_c",
                "OVERHEAT", severity, 101.0, 100.0, 3, 99.0, ts);
    }

    @Test
    void returnsNewestFirst() {
        InMemoryAlertSink sink = new InMemoryAlertSink(100);
        sink.write(alert("m1", "CRITICAL", 1000));
        sink.write(alert("m1", "CRITICAL", 2000));
        sink.write(alert("m1", "CRITICAL", 3000));

        List<Alert> recent = sink.recent(null, null, 10);

        assertEquals(3, recent.size());
        assertEquals(3000L, recent.get(0).ts(), "newest alert must come first");
        assertEquals(1000L, recent.get(2).ts());
    }

    @Test
    void capIsEnforcedAndDropsTheOldest() {
        InMemoryAlertSink sink = new InMemoryAlertSink(3);
        for (int i = 1; i <= 10; i++) {
            sink.write(alert("m1", "CRITICAL", i * 1000L));
        }
        List<Alert> recent = sink.recent(null, null, 100);

        assertEquals(3, recent.size(), "the ring must stay bounded");
        assertEquals(10_000L, recent.get(0).ts());
        assertEquals(8_000L, recent.get(2).ts(), "older alerts are evicted");
        assertEquals(10L, sink.count(), "the total counter still sees every alert");
    }

    @Test
    void filtersByDevice() {
        InMemoryAlertSink sink = new InMemoryAlertSink(100);
        sink.write(alert("m1", "CRITICAL", 1000));
        sink.write(alert("m2", "CRITICAL", 2000));

        List<Alert> recent = sink.recent("m2", null, 10);

        assertEquals(1, recent.size());
        assertEquals("m2", recent.get(0).deviceId());
    }

    @Test
    void filtersBySeverityCaseInsensitively() {
        InMemoryAlertSink sink = new InMemoryAlertSink(100);
        sink.write(alert("m1", "CRITICAL", 1000));
        sink.write(alert("m1", "WARNING", 2000));

        assertEquals(1, sink.recent(null, "critical", 10).size());
        assertEquals(1, sink.recent(null, "WARNING", 10).size());
        assertEquals(2, sink.recent(null, null, 10).size());
    }

    @Test
    void respectsTheLimit() {
        InMemoryAlertSink sink = new InMemoryAlertSink(100);
        for (int i = 1; i <= 20; i++) {
            sink.write(alert("m1", "CRITICAL", i * 1000L));
        }
        assertEquals(5, sink.recent(null, null, 5).size());
    }

    @Test
    void emptySinkIsSafeToQuery() {
        InMemoryAlertSink sink = new InMemoryAlertSink(10);
        assertTrue(sink.recent(null, null, 10).isEmpty());
        assertEquals(0L, sink.count());
    }
}
