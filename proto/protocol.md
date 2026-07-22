# fluxmesh wire protocol

One JSON contract from sensor to dashboard.

## 1. Reading (device → ingest)

```json
{
  "deviceId": "motor-017",
  "site": "plant-a",
  "metric": "temperature_c",
  "value": 104.2,
  "ts": 1752710400123
}
```

`ts` is epoch **milliseconds**. Ingest stamps arrival time when it is absent.
Over MQTT the topic is `fluxmesh/<site>/<deviceId>/<metric>` and the payload is
the same object.

## 2. Alert (stream processor → sink + dashboard)

```json
{
  "alertId": "motor-017:OVERHEAT:1752710400123",
  "deviceId": "motor-017",
  "site": "plant-a",
  "metric": "temperature_c",
  "pattern": "OVERHEAT",
  "severity": "CRITICAL",
  "triggerValue": 104.2,
  "threshold": 100.0,
  "consecutiveCount": 3,
  "windowMeanC": 101.7,
  "ts": 1752710400123
}
```

`pattern` ∈ `OVERHEAT | FLATLINE | SPIKE | DROPOUT`.
`severity` ∈ `INFO | WARNING | CRITICAL`.

## 3. Window statistics (per device, per metric)

```json
{
  "deviceId": "motor-017",
  "metric": "temperature_c",
  "count": 60,
  "mean": 98.4,
  "min": 91.0,
  "max": 104.2,
  "stdDev": 3.1,
  "windowMillis": 60000,
  "lastSeenTs": 1752710400123
}
```

## 4. GraphQL API

```graphql
type Query {
  devices(site: String): [Device!]!
  device(deviceId: ID!): Device
  alerts(deviceId: ID, severity: Severity, limit: Int = 50): [Alert!]!
  stats(deviceId: ID!, metric: String!): WindowStats
  throughput: Throughput!
}
```

Clients request exactly the fields they need — the reason GraphQL is here
rather than REST, since a dashboard tile wants `mean` only while an incident
view wants the full alert history.

## Complex event patterns

| pattern | fires when | default |
| --- | --- | --- |
| `OVERHEAT` | value > threshold for **N consecutive** readings | 100.0 °C, N=3 |
| `SPIKE` | value jumps more than `k · stdDev` above the window mean | k=4 |
| `FLATLINE` | value is unchanged for N consecutive readings (stuck sensor) | N=10 |
| `DROPOUT` | no reading from a device for longer than the timeout | 30 s |

`OVERHEAT` deliberately requires **consecutive** breaches: a single spurious
reading over the line is sensor noise, not a fault. That distinction is the
difference between an actionable alert and an ignored one.

## Ports

| service | port | protocol |
| --- | --- | --- |
| Java processor | 8080 | HTTP `/graphql`, `/graphiql`, `/actuator/health` |
| TS dashboard | 3000 | HTTP |
| MQTT broker *(optional)* | 1883 | MQTT |
| MongoDB *(optional)* | 27017 | — |

## Profiles

| profile | ingest | sink |
| --- | --- | --- |
| *(default)* | built-in device simulator | in-memory ring |
| `mqtt` | Mosquitto subscription | unchanged |
| `mongo` | unchanged | MongoDB collections |

The default profile runs the whole pipeline with **no external services**.
