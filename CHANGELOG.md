# Changelog

Format: [Keep a Changelog](https://keepachangelog.com/); versioning: [SemVer](https://semver.org/).

## [0.1.0] - 2026-07-18

Initial release — a stateful IoT telemetry stream processor with in-flight
complex event processing.

### Added
- **Sliding-window engine**: incremental sum and sum-of-squares give O(1)
  mean/stdDev per event at any window size; min/max computed lazily because they
  genuinely require a scan after the extreme is evicted.
- **Keyed state**: one window and one CEP detector per `(deviceId, metric)`,
  with lock-free lookup and per-key mutation guards, so devices never contend.
- **Complex event processing**: `OVERHEAT` (N consecutive breaches),
  `SPIKE` (k standard deviations above the window mean, guarded against
  zero-variance windows), `FLATLINE` (stuck sensor), and `DROPOUT` (evaluated on
  a timer, since nothing arrives to trigger it). Sustained faults fire once
  rather than repeatedly.
- **Pluggable ingest**: a built-in factory-floor simulator with injected faults
  (default), or real MQTT via Paho under the `mqtt` profile.
- **Pluggable sink**: a bounded in-memory alert ring (default) or MongoDB with a
  `(deviceId, ts)` compound index under the `mongo` profile.
- **Spring for GraphQL API**: `devices`, `stats`, `alerts` and `pipeline`
  queries, plus GraphiQL at `/graphiql`.
- **Next.js dashboard**: live device tiles with mean/min/max/σ and stale
  indicators, and a severity-coded alert feed, all from a single GraphQL query.
- 34 JUnit tests, Dockerfiles, docker-compose with an optional infrastructure
  profile, GitHub Actions CI, Makefile, MIT license.
