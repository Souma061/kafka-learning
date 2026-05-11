/**
 * K6 Comparison Test — Direct vs Kafka (side-by-side scenarios)
 * ==============================================================
 * Runs BOTH paths simultaneously using K6 scenarios.
 * Each scenario has its own VU pool, metrics, and thresholds.
 *
 * The terminal output shows real-time:
 *   ✅ kafka scenario  → http_req_duration stays flat ~5ms
 *   ❌ direct scenario → http_req_duration climbs, failures appear
 *
 * Run (default 30 VUs each):
 *   TEST_EMAIL=you@email.com k6 run scripts/k6/comparison_test.js
 *
 * Crank up to really see the difference:
 *   TEST_EMAIL=you@email.com k6 run --env VUS=150 scripts/k6/comparison_test.js
 *
 * Prerequisites:
 *   1. All services running:  docker compose up
 *   2. Inventory seeded:
 *        docker exec kafka-learning-redis redis-cli hset inventory product-1 99999
 *        docker exec kafka-learning-redis redis-cli hset inventory product-2 99999
 *        docker exec kafka-learning-redis redis-cli hset inventory product-3 99999
 *   3. RESEND_API_KEY set and email-service running
 */

import http from "k6/http";
import { check, sleep } from "k6";
import { Rate, Trend, Counter } from "k6/metrics";

// ── Config ───────────────────────────────────────────────────────────────────
const ORDER_URL = __ENV.ORDER_SERVICE_URL || "http://localhost:8001";
const EMAIL_URL = __ENV.EMAIL_SERVICE_URL || "http://localhost:8003";
const TEST_EMAIL = __ENV.TEST_EMAIL       || "test@example.com";
const VUS        = parseInt(__ENV.VUS     || "30");

// ── Custom metrics (one set per path) ────────────────────────────────────────
// Kafka path
const kafkaLatency = new Trend("kafka_http_duration_ms", true);
const kafkaErrors  = new Rate("kafka_error_rate");
const kafkaTotal   = new Counter("kafka_orders_total");

// Direct path
const directLatency = new Trend("direct_http_duration_ms", true);
const directErrors  = new Rate("direct_error_rate");
const directTotal   = new Counter("direct_emails_total");

// ── Scenarios ─────────────────────────────────────────────────────────────────
export const options = {
  // Stream metrics live to InfluxDB → visible in Grafana at http://localhost:3000
  // Override via: k6 run --out influxdb=http://localhost:8086/k6 ...
  ext: {
    loadimpact: { projectID: 0 },
  },

  scenarios: {
    // ── SCENARIO 1: Kafka buffered path ─────────────────────────────────────
    kafka_path: {
      executor: "ramping-vus",
      startVUs: 1,
      stages: [
        { duration: "10s", target: VUS  },   // ramp up
        { duration: "40s", target: VUS  },   // sustained load
        { duration: "10s", target: 0    },   // ramp down
      ],
      exec: "kafkaScenario",
      tags: { scenario: "kafka" },
    },

    // ── SCENARIO 2: Direct (unbuffered) path ─────────────────────────────────
    direct_path: {
      executor: "ramping-vus",
      startVUs: 1,
      stages: [
        { duration: "10s", target: VUS  },   // same ramp as Kafka
        { duration: "40s", target: VUS  },   // same sustained load
        { duration: "10s", target: 0    },
      ],
      exec: "directScenario",
      tags: { scenario: "direct" },
    },
  },

  // ── Thresholds ──────────────────────────────────────────────────────────────
  thresholds: {
    // Kafka HTTP layer should ALWAYS be fast — it's just a Kafka publish
    "kafka_http_duration_ms{scenario:kafka}":   ["p(95) < 300"],
    "kafka_error_rate{scenario:kafka}":         ["rate < 0.01"],

    // Direct path will degrade — we don't assert thresholds here,
    // just collect data to compare
    "direct_http_duration_ms{scenario:direct}": ["p(95) < 10000"],  // loose
    "direct_error_rate{scenario:direct}":       ["rate < 1.0"],      // just track
  },
};

// ── Kafka scenario VU function ────────────────────────────────────────────────
export function kafkaScenario() {
  const vuId = __VU;

  const res = http.post(
    `${ORDER_URL}/orders`,
    JSON.stringify({
      customer_email: TEST_EMAIL,
      product_id:     `product-${(vuId % 3) + 1}`,
      quantity:       1,
    }),
    {
      headers: { "Content-Type": "application/json" },
      tags:    { path: "kafka" },
    }
  );

  const ok = check(res, {
    "[kafka] 200 OK":        (r) => r.status === 200,
    "[kafka] has order_id":  (r) => {
      try { return !!JSON.parse(r.body).order_id; }
      catch { return false; }
    },
  });

  kafkaLatency.add(res.timings.duration);
  kafkaErrors.add(!ok ? 1 : 0);
  kafkaTotal.add(1);

  sleep(0.05);
}

// ── Direct scenario VU function ───────────────────────────────────────────────
export function directScenario() {
  const vuId    = __VU;
  const iterNum = __ITER;

  const res = http.post(
    `${EMAIL_URL}/direct/send-email`,
    JSON.stringify({
      to:         TEST_EMAIL,
      order_id:   `direct-${vuId}-${iterNum}`,
      product_id: `product-${(vuId % 3) + 1}`,
      quantity:   1,
      confirmed:  true,
    }),
    {
      headers: { "Content-Type": "application/json" },
      tags:    { path: "direct" },
    }
  );

  const ok = check(res, {
    "[direct] 200 OK":       (r) => r.status === 200,
    "[direct] has email_id": (r) => {
      try { return !!JSON.parse(r.body).email_id; }
      catch { return false; }
    },
  });

  directLatency.add(res.timings.duration);
  directErrors.add(!ok ? 1 : 0);
  directTotal.add(1);

  if (!ok) {
    console.warn(`[direct] ❌ VU${vuId} status=${res.status} body=${res.body.substring(0, 100)}`);
  }

  sleep(0.1);
}

// ── Summary ───────────────────────────────────────────────────────────────────
export function handleSummary(data) {
  const kd = data.metrics["kafka_http_duration_ms"];
  const dd = data.metrics["direct_http_duration_ms"];
  const ke = data.metrics["kafka_error_rate"]?.values?.rate ?? 0;
  const de = data.metrics["direct_error_rate"]?.values?.rate ?? 0;
  const kt = data.metrics["kafka_orders_total"]?.values?.count ?? 0;
  const dt = data.metrics["direct_emails_total"]?.values?.count ?? 0;

  const fmt = (v) => v != null ? `${v.toFixed(1)} ms` : "N/A";

  console.log("\n");
  console.log("╔══════════════════════════════════════════════════════╗");
  console.log("║         THROTTLING COMPARISON — FINAL RESULTS        ║");
  console.log("╠══════════════════════════════════════╦═══════════════╣");
  console.log("║ Metric                               ║ Kafka  Direct ║");
  console.log("╠══════════════════════════════════════╬═══════════════╣");
  console.log(`║ Total requests                       ║ ${String(kt).padStart(5)}  ${String(dt).padStart(5)} ║`);
  console.log(`║ HTTP p50 latency                     ║ ${fmt(kd?.values["p(50)"]).padStart(8)}  ${fmt(dd?.values["p(50)"]).padStart(8)} ║`);
  console.log(`║ HTTP p95 latency                     ║ ${fmt(kd?.values["p(95)"]).padStart(8)}  ${fmt(dd?.values["p(95)"]).padStart(8)} ║`);
  console.log(`║ HTTP p99 latency                     ║ ${fmt(kd?.values["p(99)"]).padStart(8)}  ${fmt(dd?.values["p(99)"]).padStart(8)} ║`);
  console.log(`║ HTTP max latency                     ║ ${fmt(kd?.values["max"]).padStart(8)}  ${fmt(dd?.values["max"]).padStart(8)} ║`);
  console.log(`║ Error rate                           ║ ${(ke*100).toFixed(1).padStart(5)}%  ${(de*100).toFixed(1).padStart(5)}% ║`);
  console.log("╠══════════════════════════════════════╩═══════════════╣");
  console.log("║ 🕐 Kafka emails still arriving — poll the stats:     ║");
  console.log("║    curl -s http://localhost:8003/health               ║");
  console.log("╚══════════════════════════════════════════════════════╝");

  return {};
}
