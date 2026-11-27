import * as Sentry from '@sentry/node';

import { APP_NAME, logJson } from './json-console.js';

const DEFAULT_DSN = 'http://public@host.docker.internal:8080/2';
const DEFAULT_ENVIRONMENT = 'production';
const DEFAULT_RELEASE = 'sample-apps@local';

function applyEnvironmentOverride(event) {
  const environment = event.tags?.sample_environment;
  if (environment) {
    event.environment = environment;
  }
}

function sanitizeEvent(event) {
  const { modules, sdkProcessingMetadata, ...payload } = event;
  return payload;
}

function logPayload(item, event) {
  logJson('debug', 'sentry.payload', {
    sentry_item: item,
    event_id: event.event_id,
    payload: sanitizeEvent(event),
  });
}

function beforeSend(event) {
  applyEnvironmentOverride(event);
  logPayload('event', event);
  return event;
}

function beforeSendTransaction(event) {
  applyEnvironmentOverride(event);
  logPayload('transaction', event);
  return event;
}

export function initSentry() {
  const dsn = process.env.SENTRY_DSN || DEFAULT_DSN;
  const environment =
    process.env.SENTRY_ENVIRONMENT || DEFAULT_ENVIRONMENT;
  const release = process.env.SENTRY_RELEASE || DEFAULT_RELEASE;

  Sentry.init({
    dsn,
    environment,
    release,
    debug: true,
    attachStacktrace: true,
    // Only the manually named transaction is sampled. All other automatic
    // spans are dropped at rate 0.
    tracesSampler: (samplingContext) =>
      samplingContext.name === `sample.transaction.${APP_NAME}` ? 1.0 : 0.0,
    beforeSend,
    beforeSendTransaction,
  });

  logJson('info', 'sentry.initialized', { dsn, environment, release });
}
