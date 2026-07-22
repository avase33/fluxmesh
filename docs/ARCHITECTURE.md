# fluxmesh architecture

An industrial telemetry pipeline. The bottleneck it attacks: **standard
databases collapse under sensor write load.** A thousand machines at 100 Hz is
100,000 inserts per second of data that is individually worthless and only
meaningful in aggregate.

```
        sensors (MQTT, 100 Hz each)
              │
              ▼
┌──────────────────────────┐
│ Ingest · Java            │  MqttIngest | DeviceSimulator
└───────┬──────────────────┘
        │ Reading
        ▼
┌──────────────────────────┐
│ KeyedState               │  one SlidingWindow + PatternDetector
│ partitioned by           │  per (deviceId, metric)
│ deviceId|metric          │
└───────┬──────────────────┘
        │ Match
        ▼
┌──────────────────────────┐
│ AlertSink                │  in-memory ring | MongoDB
└───────┬──────────────────┘
        │ GraphQL
        ▼
┌──────────────────────────┐
│ Dashboard · TypeScript   │
└──────────────────────────┘
```

## Why stream processing rather than store-then-query

The instinct is to insert every reading and run queries over the table. That
fails on write throughput long before it fails on query performance — and it is
solving the wrong problem, because nobody wants to see the individual readings.
They want to know that *motor-017 has been over 100 °C for three readings*.

So the analysis moves ahead of storage. Windows are maintained in memory as data
arrives, patterns are evaluated per reading, and only the alerts that fire are
written. Storage volume drops by orders of magnitude and the answer is available
in microseconds instead of after a scan.

## Keyed state

Every `(deviceId, metric)` pair gets its own sliding window and its own CEP
detector. This is the single most important design decision, and it is the same
one Flink makes.

Partitioning by key means **two devices share no state**. Their readings can be
processed on different threads, or on entirely different machines, with no
coordination and no locking between them. Only readings for the *same* key need
to be ordered relative to one another. That is what turns "process a stream"
into an embarrassingly parallel problem.

Concretely: `ConcurrentHashMap` for lock-free lookup, a per-entry monitor for
mutation. Different devices never contend; one device's updates stay
linearisable. `KeyedStateTest` runs 8 devices × 500 readings across 8 threads
and asserts the incremental statistics did not drift.

## O(1) window statistics

`SlidingWindow` keeps a deque of samples plus running `sum` and `sumSquares`,
both adjusted on insert *and* on eviction. So:

    mean = sum / n
    var  = max(sumSquares / n - mean², 0)

are O(1) per event no matter how many samples the window holds — which matters
when the window is 60 seconds of 100 Hz data.

`min` and `max` are the honest exception. Once the current extreme is evicted
there is no way to recover the next one without looking, so they are computed
lazily on read rather than pretended to be incremental. `SlidingWindowTest`
covers exactly this case.

## The CEP patterns

| pattern | rule | why |
| --- | --- | --- |
| `OVERHEAT` | N consecutive readings above threshold | one sample over the line is noise |
| `SPIKE` | jump ≥ k standard deviations above the window mean | relative to *this* device's normal variation, not a global constant |
| `FLATLINE` | value unchanged for N readings | a stuck sensor reads plausibly but is lying |
| `DROPOUT` | no reading for longer than the timeout | evaluated on a timer, since nothing arrives to trigger it |

Two subtleties worth knowing:

**Consecutive breaches, not one.** Requiring a run costs a few hundred
milliseconds of detection latency and removes most false positives. Operators
ignore noisy alerting systems, which makes a noisy system worse than none.

**Spikes are measured against the device's own variability.** A fixed threshold
cannot tell a hot machine from a broken sensor. Using standard deviations from
the window mean adapts per device automatically — with a guard for zero-variance
windows, because a perfectly flat window would otherwise make every reading
infinitely many deviations from the mean.

## Why GraphQL

The clients want genuinely different shapes of the same data. A wall-board tile
renders one number per device. An incident view needs full alert history with
thresholds and run lengths. The dashboard fetches all three top-level fields in
a single round trip.

With REST that is either several endpoints or one endpoint that over-fetches for
everybody. Here the query *is* the contract.

## Offline-first

Default profile: device simulator in, in-memory alert ring out, everything in
one process. `mvn spring-boot:run` gives a working pipeline with faults already
injected — one machine overheating, one stuck sensor, one spiking, one silent —
so every pattern is visible within a minute without configuring anything.

`docker compose --profile infra up` adds Mosquitto and MongoDB, and
`SPRING_PROFILES_ACTIVE=mqtt,mongo` swaps the two edges. The state and CEP
layers do not change.
