package com.fluxmesh;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * fluxmesh — a stateful IoT telemetry stream processor.
 *
 * <p>Readings are analysed <b>in flight</b>: windows and complex-event patterns
 * are evaluated as data arrives, and only the alerts that fire are persisted.
 * A database never sees the raw firehose.
 */
@SpringBootApplication
@EnableScheduling
public class FluxmeshApplication {

    public static void main(String[] args) {
        SpringApplication.run(FluxmeshApplication.class, args);
    }
}
