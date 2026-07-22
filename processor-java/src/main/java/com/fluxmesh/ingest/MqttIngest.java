package com.fluxmesh.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fluxmesh.model.Reading;
import com.fluxmesh.stream.StreamProcessor;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Real MQTT ingest (profile {@code mqtt}).
 *
 * <p>Subscribes to {@code fluxmesh/+/+/+} — site, device, metric — and feeds
 * every payload into the same {@link StreamProcessor} the simulator uses, so
 * the processing path is identical whether the data is real or synthetic.
 *
 * <p>QoS 0 is deliberate. Telemetry is high-rate and individually
 * disposable: a lost sample costs nothing because the window still has dozens
 * more, while QoS 1/2 acknowledgement round-trips would throttle the ingest
 * rate and buy reliability the use case does not need.
 */
@Component
@Profile("mqtt")
public class MqttIngest {

    private static final Logger log = LoggerFactory.getLogger(MqttIngest.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final StreamProcessor processor;
    private final String brokerUrl;
    private final String topicFilter;
    private MqttClient client;

    public MqttIngest(StreamProcessor processor,
                      @Value("${fluxmesh.mqtt.broker:tcp://localhost:1883}") String brokerUrl,
                      @Value("${fluxmesh.mqtt.topic:fluxmesh/+/+/+}") String topicFilter) {
        this.processor = processor;
        this.brokerUrl = brokerUrl;
        this.topicFilter = topicFilter;
    }

    @PostConstruct
    void connect() throws Exception {
        client = new MqttClient(brokerUrl, "fluxmesh-" + System.nanoTime(), new MemoryPersistence());
        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(10);
        client.connect(options);

        client.subscribe(topicFilter, 0, (topic, message) -> {
            try {
                processor.onReading(parse(topic, message.getPayload()));
            } catch (Exception e) {
                log.debug("dropping malformed message on {}: {}", topic, e.toString());
            }
        });
        log.info("MQTT ingest connected to {} subscribed to {}", brokerUrl, topicFilter);
    }

    /**
     * Builds a reading from the payload, falling back to the topic segments
     * for any field the publisher left out.
     */
    static Reading parse(String topic, byte[] payload) throws Exception {
        JsonNode node = MAPPER.readTree(new String(payload, StandardCharsets.UTF_8));
        String[] parts = topic.split("/");

        String site = node.path("site").asText(parts.length > 1 ? parts[1] : "default");
        String deviceId = node.path("deviceId").asText(parts.length > 2 ? parts[2] : "unknown");
        String metric = node.path("metric").asText(parts.length > 3 ? parts[3] : "value");

        return new Reading(deviceId, site, metric,
                node.path("value").asDouble(),
                node.path("ts").asLong(0L));
    }

    @PreDestroy
    void disconnect() {
        try {
            if (client != null && client.isConnected()) {
                client.disconnect();
            }
        } catch (Exception e) {
            log.debug("mqtt disconnect: {}", e.toString());
        }
    }
}
