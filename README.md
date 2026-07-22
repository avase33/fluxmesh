# fluxmesh 🏭

**A stateful IoT telemetry stream processor.** Thousands of sensors stream in,
and the analysis happens **in flight** — sliding windows and complex-event
patterns are evaluated as data arrives, and only the alerts that actually fire
are stored. The raw firehose never touches a database.

```
sensors ──MQTT──▶ ingest ──▶ keyed state ──▶ CEP patterns ──▶ alert sink
                              (per device)   (overheat/spike/    (memory|Mongo)
                                              flatline/dropout)        │
                                                                       ▼
                                              TypeScript dashboard ◀─ GraphQL
```

| Layer | Technology | Owns |
| --- | --- | --- |
| **Ingest** | Java 21 | MQTT subscription, or a built-in device simulator |
| **State** | Pure Java | Per-device sliding windows, O(1) rolling statistics |
| **CEP** | Pure Java | Consecutive-breach, spike, flatline and dropout patterns |
| **Sink** | In-memory ring *(default)* · MongoDB | Alert history |
| **API** | Spring for GraphQL | Clients fetch exactly the fields they render |
| **Dashboard** | TypeScript · Next.js | Live device tiles + alert feed |

**The default profile runs the whole pipeline with no external services** — no
broker, no database. A device simulator generates a factory floor complete with
injected faults, so the dashboard is alive on first boot.

## Quickstart

```bash
cd processor-java && mvn spring-boot:run       # :8080, simulator running
cd dashboard-ts && npm install && npm run dev  # :3000
```

Open **http://localhost:8080/graphiql** and try:

```graphql
{
  pipeline { readingsPerSecond activeKeys alertsFired }
  devices(site: "plant-a") { deviceId mean max stale }
  alerts(severity: CRITICAL, limit: 10) {
    deviceId pattern triggerValue consecutiveCount
  }
}
```

The simulator deliberately injects one machine that overheats gradually, one
stuck sensor, one that throws lone spikes, and one that goes silent — so within
a minute you should see `OVERHEAT`, `FLATLINE`, `SPIKE` and `DROPOUT` all fire.

## Running against real infrastructure

```bash
docker compose --profile infra up --build      # Mosquitto + MongoDB
SPRING_PROFILES_ACTIVE=mqtt,mongo mvn spring-boot:run
```

Publish to `fluxmesh/<site>/<device>/<metric>`:

```bash
mosquitto_pub -t fluxmesh/plant-a/motor-017/temperature_c \
  -m '{"value":104.2}'
```

Nothing in the state, CEP or API layers changes — ingest and sink are interfaces.

## The interesting engineering

- **Keyed state** — one window and one detector per `deviceId|metric`.
  Partitioning by key is what makes this parallelisable: two devices share
  nothing, so they can run on different threads or different machines with no
  coordination. Lookup is lock-free; mutation is guarded per key.
  `stream/KeyedState.java`
- **O(1) rolling statistics** — sum and sum-of-squares are updated incrementally
  on insert *and* eviction, so mean and standard deviation are constant-time at
  any window size. `min`/`max` are the honest exception and are computed lazily,
  because they genuinely need a scan once the extreme is evicted.
  `stream/SlidingWindow.java`
- **Consecutive-breach CEP** — `OVERHEAT` requires N readings in a row above the
  threshold, not one. A single sample over the line is sensor noise; requiring a
  run trades a little detection latency for a large drop in false positives.
  An alerting system people ignore is worse than none. `cep/PatternDetector.java`
- **GraphQL over REST** — a wall-board tile wants one number per device while an
  incident view wants full alert history. One query, no over-fetching.

## Testing

```bash
make test        # or: cd processor-java && mvn test
```

34 tests covering window eviction and min/max recovery, incremental-statistics
drift over 5,000 events, consecutive-breach semantics, `>` vs `>=` at the
threshold, sustained faults firing once rather than repeatedly, zero-variance
windows not producing infinite spikes, dropout timing, per-device state
isolation, and 8 threads × 500 readings without corrupting state.

## A note on Flink

The blueprint for this project called for Apache Flink. This implements the
same concepts — keyed state, sliding windows, complex event processing — **by
hand**, and deliberately.

Flink's real value is distribution, checkpointing and exactly-once semantics
across a cluster, not the arithmetic. Writing the arithmetic directly means the
project runs with `mvn spring-boot:run` and no cluster, and it makes the
mechanics inspectable rather than hidden behind an operator DSL. If you were
scaling this past one machine you would port `KeyedState` and `PatternDetector`
into a Flink `KeyedProcessFunction` almost line for line — the state model is
intentionally the same shape.

## Layout

```
proto/protocol.md   JSON + GraphQL contract
processor-java/     stream engine, CEP, ingest, sinks, GraphQL API
dashboard-ts/       Next.js device tiles + alert feed
docs/ARCHITECTURE.md
```

## License

MIT © 2026 Akhil Vase
