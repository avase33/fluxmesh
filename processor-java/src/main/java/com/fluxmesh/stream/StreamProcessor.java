package com.fluxmesh.stream;

import com.fluxmesh.cep.PatternDetector;
import com.fluxmesh.model.Alert;
import com.fluxmesh.model.Reading;
import com.fluxmesh.sink.AlertSink;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * The streaming job: reading in, keyed state updated, patterns evaluated,
 * alerts out.
 *
 * <p>Everything happens <b>in flight</b>. Nothing is written to a database in
 * order to be analysed; only the alerts that actually fired are persisted.
 * That inversion is the whole point — thousands of sensors at 100 Hz will
 * overwhelm a database's write path long before they trouble the arithmetic,
 * so the analysis has to happen before storage rather than after it.
 */
@Component
public class StreamProcessor {

    private static final Logger log = LoggerFactory.getLogger(StreamProcessor.class);

    private final KeyedState state;
    private final AlertSink sink;
    private final AtomicLong processed = new AtomicLong();
    private final AtomicLong alertsFired = new AtomicLong();
    private volatile long startedAt;

    public StreamProcessor(
            AlertSink sink,
            @Value("${fluxmesh.window-millis:60000}") long windowMillis,
            @Value("${fluxmesh.cep.overheat-threshold:100.0}") double overheatThreshold,
            @Value("${fluxmesh.cep.overheat-consecutive:3}") int overheatConsecutive,
            @Value("${fluxmesh.cep.spike-std-devs:4.0}") double spikeStdDevs,
            @Value("${fluxmesh.cep.flatline-consecutive:10}") int flatlineConsecutive,
            @Value("${fluxmesh.cep.dropout-millis:30000}") long dropoutMillis) {
        this.sink = sink;
        this.state = new KeyedState(windowMillis, new PatternDetector.Config(
                overheatThreshold, overheatConsecutive, spikeStdDevs,
                flatlineConsecutive, dropoutMillis));
    }

    @PostConstruct
    void init() {
        startedAt = System.currentTimeMillis();
        log.info("stream processor ready (sink={})", sink.name());
    }

    /** Processes one reading. Safe to call from many threads at once. */
    public void onReading(Reading raw) {
        Reading reading = raw.normalized(System.currentTimeMillis());
        KeyedState.Processed result = state.apply(reading);
        processed.incrementAndGet();

        for (PatternDetector.Match match : result.matches()) {
            Alert alert = Alert.from(reading, match, result.windowMean());
            sink.write(alert);
            alertsFired.incrementAndGet();
            log.info("alert {} {} {}={} (window mean {})",
                    alert.severity(), alert.pattern(), alert.deviceId(),
                    alert.triggerValue(), alert.windowMean());
        }
    }

    /** Fires DROPOUT for devices that have stopped reporting. */
    @Scheduled(fixedDelayString = "${fluxmesh.dropout-sweep-millis:10000}")
    public void sweepDropouts() {
        long now = System.currentTimeMillis();
        for (KeyedState.DropoutEvent event : state.sweepDropouts(now)) {
            Reading synthetic = new Reading(
                    event.deviceId(), event.site(), event.metric(), 0.0, now);
            Alert alert = Alert.from(synthetic, event.match(), 0.0);
            sink.write(alert);
            alertsFired.incrementAndGet();
            log.warn("alert {} {} {} silent", alert.severity(), alert.pattern(), alert.deviceId());
        }
    }

    public KeyedState state() {
        return state;
    }

    public long processed() {
        return processed.get();
    }

    public long alertsFired() {
        return alertsFired.get();
    }

    public double readingsPerSecond() {
        long elapsed = Math.max(System.currentTimeMillis() - startedAt, 1);
        return Math.round(processed.get() / (elapsed / 1000.0) * 10.0) / 10.0;
    }
}
