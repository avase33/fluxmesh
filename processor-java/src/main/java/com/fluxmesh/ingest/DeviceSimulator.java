package com.fluxmesh.ingest;

import com.fluxmesh.model.Reading;
import com.fluxmesh.stream.StreamProcessor;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A synthetic factory floor — the offline default ingest.
 *
 * <p>Generates a fleet of machines whose temperature normally wanders around a
 * setpoint, then injects the specific faults the CEP layer exists to catch:
 * one machine overheats gradually, one sensor sticks, one throws a lone spike,
 * and one goes silent. Without this the dashboard would be empty on first boot
 * and the patterns would be untestable by eye.
 */
@Component
@Profile("!mqtt")
public class DeviceSimulator {

    private static final Logger log = LoggerFactory.getLogger(DeviceSimulator.class);

    private final StreamProcessor processor;
    private final int deviceCount;
    private final long intervalMillis;
    private final Random rng = new Random(7);
    private final List<Device> devices = new ArrayList<>();
    private ScheduledExecutorService scheduler;
    private long tick;

    /** Per-machine simulation state. */
    private static final class Device {
        String id;
        String site;
        double setpoint;
        Behaviour behaviour;
        double current;
    }

    private enum Behaviour {
        NORMAL,      // wanders around the setpoint
        OVERHEATING, // climbs steadily past the threshold
        STUCK,       // sensor frozen on one value
        SPIKY,       // occasional lone outlier
        SILENT       // stops reporting after a while
    }

    public DeviceSimulator(
            StreamProcessor processor,
            @Value("${fluxmesh.simulator.devices:12}") int deviceCount,
            @Value("${fluxmesh.simulator.interval-millis:250}") long intervalMillis) {
        this.processor = processor;
        this.deviceCount = Math.max(deviceCount, 1);
        this.intervalMillis = Math.max(intervalMillis, 20);
    }

    @PostConstruct
    void start() {
        for (int i = 0; i < deviceCount; i++) {
            Device d = new Device();
            d.id = String.format("motor-%03d", i);
            d.site = i % 2 == 0 ? "plant-a" : "plant-b";
            d.setpoint = 70 + rng.nextInt(15);
            d.current = d.setpoint;
            d.behaviour = switch (i) {
                case 3 -> Behaviour.OVERHEATING;
                case 5 -> Behaviour.STUCK;
                case 7 -> Behaviour.SPIKY;
                case 9 -> Behaviour.SILENT;
                default -> Behaviour.NORMAL;
            };
            devices.add(d);
        }

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "device-simulator");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::emit, intervalMillis, intervalMillis,
                TimeUnit.MILLISECONDS);
        log.info("device simulator started: {} devices every {}ms", deviceCount, intervalMillis);
    }

    private void emit() {
        try {
            tick++;
            long now = System.currentTimeMillis();
            for (Device d : devices) {
                if (d.behaviour == Behaviour.SILENT && tick > 40) {
                    continue;   // stops reporting -> DROPOUT should fire
                }
                double value = nextValue(d);
                processor.onReading(new Reading(d.id, d.site, "temperature_c", value, now));
            }
        } catch (RuntimeException e) {
            log.warn("simulator tick failed: {}", e.toString());
        }
    }

    private double nextValue(Device d) {
        return switch (d.behaviour) {
            case NORMAL -> d.setpoint + rng.nextGaussian() * 1.5;
            case STUCK -> 61.0;                       // frozen -> FLATLINE
            case OVERHEATING -> {
                d.current += 0.6;                     // climbs past 100 -> OVERHEAT
                yield d.current;
            }
            case SPIKY -> tick % 37 == 0
                    ? d.setpoint + 60                 // lone outlier -> SPIKE
                    : d.setpoint + rng.nextGaussian() * 0.4;
            case SILENT -> d.setpoint + rng.nextGaussian();
        };
    }

    @PreDestroy
    void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }
}
