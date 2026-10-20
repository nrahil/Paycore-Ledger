import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    local_payment_flow: {
      executor: 'constant-vus',
      vus: 2,
      duration: '30s',
      gracefulStop: '5s',
    },
  },
  thresholds: {
    checks: ['rate>0.95'],
    http_req_failed: ['rate<0.05'],
    'http_req_duration{step:capture}': ['p(95)<2000'],
  },
};

const BASE_URL = __ENV.PAYCORE_URL || 'http://localhost:8090';
const API_KEY = __ENV.PAYCORE_API_KEY;

if (!API_KEY) {
  throw new Error('Set PAYCORE_API_KEY to a valid merchant API key before running this scenario.');
}

export default function () {
  const requestId = `k6-local-${__VU}-${__ITER}-${Date.now()}`;
  const headers = {
    'Content-Type': 'application/json',
    'X-API-Key': API_KEY,
  };

  const create = http.post(
    `${BASE_URL}/v1/payments`,
    JSON.stringify({ amount: 1000, currency: 'INR' }),
    {
      headers: { ...headers, 'X-Idempotency-Key': `${requestId}-create` },
      tags: { step: 'create' },
    }
  );
  if (!check(create, { 'payment created': (r) => r.status === 201 })) return;

  const paymentId = JSON.parse(create.body).id;
  const authorize = http.post(
    `${BASE_URL}/v1/payments/${paymentId}/authorize`,
    '{}',
    {
      headers: { ...headers, 'X-Idempotency-Key': `${requestId}-authorize` },
      tags: { step: 'authorize' },
    }
  );
  if (!check(authorize, { 'payment authorized': (r) => r.status === 200 })) return;

  const capture = http.post(
    `${BASE_URL}/v1/payments/${paymentId}/capture`,
    '{}',
    {
      headers: { ...headers, 'X-Idempotency-Key': `${requestId}-capture` },
      tags: { step: 'capture' },
    }
  );
  check(capture, { 'payment captured': (r) => r.status === 200 });
}
