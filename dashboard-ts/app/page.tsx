"use client";

import { useCallback, useEffect, useState } from "react";

const GRAPHQL = process.env.NEXT_PUBLIC_GRAPHQL_URL || "http://localhost:8080/graphql";

type DeviceStats = {
  deviceId: string;
  site: string;
  metric: string;
  count: number;
  mean: number;
  min: number;
  max: number;
  stdDev: number;
  lastSeenTs: number;
  stale: boolean;
};

type Alert = {
  alertId: string;
  deviceId: string;
  pattern: string;
  severity: string;
  triggerValue: number;
  threshold: number;
  consecutiveCount: number;
  windowMean: number;
  ts: number;
};

type Pipeline = {
  processed: number;
  alertsFired: number;
  readingsPerSecond: number;
  activeKeys: number;
  sink: string;
};

// One query for the whole page — the reason this API is GraphQL rather than
// REST. Three REST endpoints become one round trip, and each tile asks only
// for the fields it renders.
const DASHBOARD_QUERY = `
  query Dashboard {
    pipeline { processed alertsFired readingsPerSecond activeKeys sink }
    devices { deviceId site metric count mean min max stdDev lastSeenTs stale }
    alerts(limit: 25) {
      alertId deviceId pattern severity triggerValue threshold
      consecutiveCount windowMean ts
    }
  }
`;

function severityColor(s: string): string {
  if (s === "CRITICAL") return "#ff5c6c";
  if (s === "WARNING") return "#ffb454";
  return "#58a6ff";
}

export default function Dashboard() {
  const [pipeline, setPipeline] = useState<Pipeline | null>(null);
  const [devices, setDevices] = useState<DeviceStats[]>([]);
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [status, setStatus] = useState("connecting");

  const load = useCallback(async () => {
    try {
      const res = await fetch(GRAPHQL, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ query: DASHBOARD_QUERY }),
      });
      const body = await res.json();
      if (body.errors) {
        setStatus("error");
        return;
      }
      setPipeline(body.data.pipeline);
      setDevices(body.data.devices ?? []);
      setAlerts(body.data.alerts ?? []);
      setStatus("live");
    } catch {
      setStatus("offline");
    }
  }, []);

  useEffect(() => {
    load();
    const timer = setInterval(load, 2000);
    return () => clearInterval(timer);
  }, [load]);

  return (
    <main style={{ padding: 24, maxWidth: 1200, margin: "0 auto" }}>
      <header style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
        <h1 style={{ fontSize: 22, margin: 0 }}>fluxmesh · telemetry</h1>
        <span style={{ color: status === "live" ? "#4ec9b0" : "#ff5c6c", fontSize: 13 }}>
          {status === "live" ? "● live" : `○ ${status}`}
          {pipeline && ` · sink: ${pipeline.sink}`}
        </span>
      </header>

      <section
        style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 12, marginTop: 20 }}
      >
        <Metric label="readings / sec" value={(pipeline?.readingsPerSecond ?? 0).toLocaleString()} />
        <Metric label="processed" value={(pipeline?.processed ?? 0).toLocaleString()} />
        <Metric label="state partitions" value={(pipeline?.activeKeys ?? 0).toLocaleString()} />
        <Metric
          label="alerts fired"
          value={(pipeline?.alertsFired ?? 0).toLocaleString()}
          warn={(pipeline?.alertsFired ?? 0) > 0}
        />
      </section>

      <section style={{ marginTop: 26 }}>
        <h2 style={{ fontSize: 14, color: "#8b949e" }}>devices ({devices.length})</h2>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(230px,1fr))", gap: 10 }}>
          {devices.map((d) => (
            <div
              key={`${d.deviceId}-${d.metric}`}
              style={{
                background: "#0d1117",
                border: `1px solid ${d.stale ? "#ff5c6c" : "#30363d"}`,
                borderRadius: 8,
                padding: 12,
              }}
            >
              <div style={{ display: "flex", justifyContent: "space-between" }}>
                <b>{d.deviceId}</b>
                <span style={{ color: "#8b949e", fontSize: 11 }}>{d.site}</span>
              </div>
              <div style={{ fontSize: 26, marginTop: 6, color: d.stale ? "#ff5c6c" : "#e6edf3" }}>
                {d.mean.toFixed(1)}
                <span style={{ fontSize: 12, color: "#8b949e" }}> mean</span>
              </div>
              <div style={{ color: "#8b949e", fontSize: 12, marginTop: 4 }}>
                min {d.min.toFixed(1)} · max {d.max.toFixed(1)} · σ {d.stdDev.toFixed(2)}
              </div>
              <div style={{ color: "#8b949e", fontSize: 11, marginTop: 2 }}>
                {d.count} in window {d.stale && " · SILENT"}
              </div>
            </div>
          ))}
          {devices.length === 0 && (
            <p style={{ color: "#8b949e" }}>waiting for telemetry…</p>
          )}
        </div>
      </section>

      <section style={{ marginTop: 26 }}>
        <h2 style={{ fontSize: 14, color: "#8b949e" }}>alerts ({alerts.length})</h2>
        <div style={{ border: "1px solid #30363d", borderRadius: 8 }}>
          {alerts.length === 0 && (
            <p style={{ padding: 16, color: "#8b949e" }}>no alerts yet…</p>
          )}
          {alerts.map((a, i) => (
            <div
              key={a.alertId}
              style={{
                display: "grid",
                gridTemplateColumns: "90px 100px 100px 1fr 150px",
                gap: 8,
                padding: "8px 12px",
                borderTop: i ? "1px solid #21262d" : "none",
                alignItems: "center",
                fontSize: 13,
              }}
            >
              <span style={{ color: severityColor(a.severity), fontWeight: 700 }}>
                {a.severity}
              </span>
              <span>{a.pattern}</span>
              <span>{a.deviceId}</span>
              <span style={{ color: "#8b949e" }}>
                value {a.triggerValue} vs threshold {a.threshold}
                {a.consecutiveCount > 1 && ` · ${a.consecutiveCount} consecutive`}
              </span>
              <span style={{ color: "#8b949e", textAlign: "right" }}>
                {new Date(a.ts).toLocaleTimeString()}
              </span>
            </div>
          ))}
        </div>
      </section>
    </main>
  );
}

function Metric({ label, value, warn }: { label: string; value: string; warn?: boolean }) {
  return (
    <div
      style={{
        background: "#0d1117",
        border: `1px solid ${warn ? "#ffb454" : "#30363d"}`,
        borderRadius: 8,
        padding: 16,
      }}
    >
      <div style={{ color: "#8b949e", fontSize: 12 }}>{label}</div>
      <div style={{ fontSize: 26, marginTop: 6 }}>{value}</div>
    </div>
  );
}
