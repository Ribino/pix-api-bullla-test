import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const RATE = parseInt(__ENV.RATE || '50', 10);
const DURATION = __ENV.DURATION || '60s';

export const options = {
  scenarios: {
    pix_load: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: 20,
      maxVUs: 100,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
};

export default function () {
  const transactionId =
    `tx-k6-${Date.now()}-${exec.vu.idInTest}-${exec.vu.iterationInInstance}-${Math.floor(Math.random() * 1000000)}`;
  const payload = JSON.stringify({
    transactionId: transactionId,
    amount: 150.75,
    pixKey: `k6-user-${exec.vu.idInTest}@email.com`,
    description: 'k6 load test',
  });

  const res = http.post(`${BASE_URL}/pix`, payload, {
    headers: { 'Content-Type': 'application/json' },
    tags: { name: 'POST /pix' },
  });

  check(res, {
    'status is 202': (r) => r.status === 202,
    'has transactionId': (r) => r.json('transactionId') === transactionId,
    'status PROCESSING': (r) => r.json('status') === 'PROCESSING',
  });
}
