/**
 * K6 Load Test — Kafka Path
 * ==========================
 * Blasts order-service with concurrent POST /orders requests.
 * The HTTP call returns instantly (order goes to Kafka).
 * Email delivery happens asynchronously via the consumer.
 *
 * What to observe:
 *   - http_req_duration stays LOW regardless of concurrency (just Kafka publish)
 *   - Zero 429s — Resend is never overwhelmed
 *   - After the test: watch email-service logs drip steadily at rate-limit pace
 *
 * Run:
 *   TEST_EMAIL=you@email.com k6 run scripts/k6/kafka_test.js
 *
 * Extreme test:
 *   TEST_EMAIL=you@email.com k6 run --env MAX_VUS=500 scripts/k6/kafka_test.js
 *
 * After running, watch emails arrive:
 *   watch -n1 'curl -s http://localhost:8003/health | python3 -m json.tool'
 */

import http from "k6/http";
import { check, sleep } from "k6";
import { Rate, Trend, Counter } from "k6/metrics";

// ── Custom metrics ───────────────────────────────────────────────────────────
const errorRate       = new Rate("order_error_rate");
const orderLatency    = new Trend("order_placement_duration_ms", true);
const ordersPlaced    = new Counter("orders_placed_total");
const ordersFailed    = new Counter("orders_failed_total");

// ── Config ───────────────────────────────────────────────────────────────────
const ORDER_SERVICE_URL = __ENV.ORDER_SERVICE_URL || "http://localhost:8001";
const TEST_EMAIL        = __ENV.TEST_EMAIL        || "test@example.com";
const MAX_VUS           = parseInt(__ENV.MAX_VUS   || "50");

// ── Scenario ─────────────────────────────────────────────────────────────────
export const options = {
  scenarios: {
    kafka_order_blast: {
      executor: "ramping-vus",
      startVUs: 1,
      stages: [
        { duration: "10s", target: MAX_VUS },    // ramp up fast
        { duration: "30s", target: MAX_VUS },    // hold at peak
        { duration: "10s", target: 0       },    // ramp down
      ],
    },
  },

  thresholds: {
    // Order placement should always be fast — fail if p95 > 500ms
    order_placement_duration_ms: ["p(95) < 500"],
    // Should never fail — Kafka absorbs all load
    order_error_rate:            ["rate < 0.01"],
    http_req_failed:             ["rate < 0.01"],
  },
};

// ── Main VU function ─────────────────────────────────────────────────────────
export default function () {
  const vuId    = __VU;
  const iterNum = __ITER;

  const payload = JSON.stringify({
    customer_email: TEST_EMAIL,
    product_id:     `product-${(vuId % 3) + 1}`,
    quantity:       1,
  });

  const params = {
    headers: { "Content-Type": "application/json" },
    tags:    { path: "kafka" },
  };

  const res = http.post(`${ORDER_SERVICE_URL}/orders`, payload, params);

  const ok = check(res, {
    "status is 200":          (r) => r.status === 200,
    "has order_id in body":   (r) => {
      try { return JSON.parse(r.body).order_id !== undefined; }
      catch { return false; }
    },
  });

  orderLatency.add(res.timings.duration);

  if (ok) {
    ordersPlaced.add(1);
    errorRate.add(0);
  } else {
    ordersFailed.add(1);
    errorRate.add(1);
    console.warn(`❌ [VU${vuId}] status=${res.status} body=${res.body.substring(0, 120)}`);
  }

  sleep(0.05);
}

// ── Summary hook ─────────────────────────────────────────────────────────────
export function handleSummary(data) {
  const dur    = data.metrics["order_placement_duration_ms"];
  const placed = data.metrics["orders_placed_total"]?.values?.count ?? 0;
  const failed = data.metrics["orders_failed_total"]?.values?.count ?? 0;

  console.log("\n📊 KAFKA PATH SUMMARY (HTTP layer only)");
  console.log(`   Orders placed  : ${placed}`);
  console.log(`   Orders failed  : ${failed}`);
  if (dur) {
    console.log(`   HTTP p50       : ${dur.values["p(50)"]?.toFixed(1)} ms  ← should be ~5ms`);
    console.log(`   HTTP p95       : ${dur.values["p(95)"]?.toFixed(1)} ms`);
    console.log(`   HTTP max       : ${dur.values["max"]?.toFixed(1)} ms`);
  }
  console.log(`\n   ⏳ ${placed} emails will arrive at rate-limit pace.`);
  console.log(`   Watch: watch -n1 'curl -s http://localhost:8003/health'`);
  return {};
}
