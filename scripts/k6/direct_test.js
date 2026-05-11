/**
 * K6 Load Test — Direct API Path
 * ================================
 * Sends emails by calling the email-service /direct/send-email endpoint directly.
 * This bypasses Kafka entirely — Resend is called synchronously inside each HTTP request.
 *
 * What to observe:
 *   - http_req_duration climbs as concurrency increases (Resend latency blocks the loop)
 *   - http_req_failed spikes when Resend returns 429 Too Many Requests
 *   - error_rate threshold will breach at high VU counts
 *
 * Run:
 *   TEST_EMAIL=you@email.com k6 run scripts/k6/direct_test.js
 *
 * Ramp up aggressively:
 *   TEST_EMAIL=you@email.com k6 run --env MAX_VUS=100 scripts/k6/direct_test.js
 */

import http from "k6/http";
import { check, sleep } from "k6";
import { Rate, Trend, Counter } from "k6/metrics";

// ── Custom metrics ───────────────────────────────────────────────────────────
const errorRate      = new Rate("email_error_rate");
const emailLatency   = new Trend("email_send_duration_ms", true);  // true = display as ms
const emailsSent     = new Counter("emails_sent_total");
const emailsFailed   = new Counter("emails_failed_total");

// ── Config ───────────────────────────────────────────────────────────────────
const EMAIL_SERVICE_URL = __ENV.EMAIL_SERVICE_URL || "http://localhost:8003";
const TEST_EMAIL        = __ENV.TEST_EMAIL        || "test@example.com";
const MAX_VUS           = parseInt(__ENV.MAX_VUS  || "50");

// ── Scenario: ramp up → hold → ramp down ────────────────────────────────────
export const options = {
  scenarios: {
    direct_email_blast: {
      executor: "ramping-vus",
      startVUs: 1,
      stages: [
        { duration: "15s", target: MAX_VUS },    // ramp up
        { duration: "30s", target: MAX_VUS },    // hold at peak
        { duration: "10s", target: 0    },       // ramp down
      ],
    },
  },

  thresholds: {
    // Fail the test if >5% of direct emails error
    email_error_rate:           ["rate < 0.05"],
    // Fail if p95 latency exceeds 5 seconds
    email_send_duration_ms:     ["p(95) < 5000"],
    // Standard HTTP error rate
    http_req_failed:            ["rate < 0.10"],
  },
};

// ── Main VU function ─────────────────────────────────────────────────────────
export default function () {
  const vuId    = __VU;
  const iterNum = __ITER;
  const orderId = `direct-${vuId}-${iterNum}`;

  const payload = JSON.stringify({
    to:         TEST_EMAIL,
    order_id:   orderId,
    product_id: `product-${(vuId % 3) + 1}`,
    quantity:   1,
    confirmed:  true,
  });

  const params = {
    headers: { "Content-Type": "application/json" },
    tags:    { path: "direct" },
  };

  const res = http.post(`${EMAIL_SERVICE_URL}/direct/send-email`, payload, params);

  const ok = check(res, {
    "status is 200":          (r) => r.status === 200,
    "has email_id in body":   (r) => {
      try { return JSON.parse(r.body).email_id !== undefined; }
      catch { return false; }
    },
  });

  emailLatency.add(res.timings.duration);

  if (ok) {
    emailsSent.add(1);
    errorRate.add(0);
  } else {
    emailsFailed.add(1);
    errorRate.add(1);
    console.warn(`❌ [VU${vuId}] status=${res.status} body=${res.body.substring(0, 120)}`);
  }

  // Small think-time so VUs don't spin at 100% CPU
  sleep(0.1);
}

// ── Summary hook ─────────────────────────────────────────────────────────────
export function handleSummary(data) {
  const dur = data.metrics["email_send_duration_ms"];
  console.log("\n📊 DIRECT PATH SUMMARY");
  console.log(`   Emails sent    : ${data.metrics["emails_sent_total"]?.values?.count ?? 0}`);
  console.log(`   Emails failed  : ${data.metrics["emails_failed_total"]?.values?.count ?? 0}`);
  if (dur) {
    console.log(`   Latency p50    : ${dur.values["p(50)"]?.toFixed(1)} ms`);
    console.log(`   Latency p95    : ${dur.values["p(95)"]?.toFixed(1)} ms`);
    console.log(`   Latency p99    : ${dur.values["p(99)"]?.toFixed(1)} ms`);
    console.log(`   Latency max    : ${dur.values["max"]?.toFixed(1)} ms`);
  }
  return {};
}
