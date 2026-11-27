import { randomUUID } from 'node:crypto';

import closeWithGrace from 'close-with-grace';
import Fastify from 'fastify';
import * as Sentry from '@sentry/node';

import { APP_NAME, installJsonConsole, logJson } from './json-console.js';
import { initSentry } from './sentry.js';

installJsonConsole();
initSentry();

const port = Number(process.env.PORT || 3000);
const defaultEnvironment =
  process.env.SENTRY_ENVIRONMENT || 'sample-apps';

const querystringSchema = {
  type: 'object',
  properties: {
    count: { type: 'integer', minimum: 1, maximum: 1000, default: 1 },
    environment: { type: 'string', minLength: 1 },
  },
  additionalProperties: false,
};

const healthResponseSchema = {
  type: 'object',
  properties: {
    status: { type: 'string' },
    app: { type: 'string' },
  },
  required: ['status', 'app'],
  additionalProperties: false,
};

const eventResponseSchema = {
  type: 'object',
  properties: {
    app: { type: 'string' },
    kind: { const: 'event' },
    count: { type: 'integer' },
    environment: { type: 'string' },
    request_id: { type: 'string' },
    event_ids: { type: 'array', items: { type: 'string' } },
    flushed: { type: 'boolean' },
  },
  required: [
    'app',
    'kind',
    'count',
    'environment',
    'request_id',
    'event_ids',
    'flushed',
  ],
  additionalProperties: false,
};

const transactionResponseSchema = {
  type: 'object',
  properties: {
    app: { type: 'string' },
    kind: { const: 'transaction' },
    count: { type: 'integer' },
    environment: { type: 'string' },
    request_id: { type: 'string' },
    trace_ids: { type: 'array', items: { type: 'string' } },
    flushed: { type: 'boolean' },
  },
  required: [
    'app',
    'kind',
    'count',
    'environment',
    'request_id',
    'trace_ids',
    'flushed',
  ],
  additionalProperties: false,
};

const app = Fastify({
  logger: {
    level: process.env.LOG_LEVEL || 'debug',
    base: { app: APP_NAME },
    timestamp: () => `,"time":"${new Date().toISOString()}"`,
    formatters: {
      level: (label) => ({ level: label }),
    },
    redact: {
      paths: [
        'req.headers.authorization',
        'req.headers.cookie',
        '*.password',
        '*.token',
        '*.secret',
      ],
      censor: '[REDACTED]',
    },
  },
  disableRequestLogging: true,
  connectionTimeout: 30000,
  keepAliveTimeout: 72000,
  bodyLimit: 1048576,
});

app.addHook('onRequest', async (request) => {
  request.requestId = request.headers['x-request-id'] || randomUUID();
  request.startedAt = process.hrtime.bigint();
  request.log.debug(
    {
      request_id: request.requestId,
      method: request.method,
      path: request.url,
    },
    'http.request.started',
  );
});

app.addHook('onResponse', async (request, reply) => {
  const durationMs =
    Number(process.hrtime.bigint() - request.startedAt) / 1_000_000;
  request.log.debug(
    {
      request_id: request.requestId,
      method: request.method,
      path: request.url,
      status: reply.statusCode,
      duration_ms: Math.round(durationMs * 1000) / 1000,
    },
    'http.request.finished',
  );
});

app.setErrorHandler((error, request, reply) => {
  request.log.error(
    { err: error, request_id: request.requestId },
    'http.request.failed',
  );

  const statusCode = error.statusCode ?? 500;
  const message =
    statusCode >= 500 ? 'Internal Server Error' : error.message;

  return reply.code(statusCode).send({
    statusCode,
    error: error.code ?? 'INTERNAL_ERROR',
    message,
  });
});

app.setNotFoundHandler((request, reply) => {
  return reply.code(404).send({
    statusCode: 404,
    error: 'NOT_FOUND',
    message: `Route ${request.method} ${request.url} not found`,
  });
});

app.get(
  '/health',
  { schema: { response: { 200: healthResponseSchema } } },
  async () => ({ status: 'ok', app: APP_NAME }),
);

app.get(
  '/events',
  {
    schema: {
      querystring: querystringSchema,
      response: { 200: eventResponseSchema },
    },
  },
  async (request) => {
    const count = request.query.count;
    const environment = request.query.environment || defaultEnvironment;
    const eventIds = [];

    request.log.debug(
      {
        request_id: request.requestId,
        count,
        environment,
      },
      'sample.events.requested',
    );

    for (let index = 0; index < count; index += 1) {
      Sentry.withScope((scope) => {
        scope.setTag('sample_app', APP_NAME);
        scope.setTag('sample_environment', environment);
        scope.setTag('sample_request_id', request.requestId);
        scope.setExtra('event_index', index);

        const eventId = Sentry.captureException(
          new Error(
            `sample ${APP_NAME} event ${index} request_id=${request.requestId}`,
          ),
        );
        eventIds.push(eventId);

        request.log.debug(
          {
            request_id: request.requestId,
            event_id: eventId,
            event_index: index,
            environment,
          },
          'sentry.capture.event',
        );
      });
    }

    const flushed = await Sentry.flush(5000);
    request.log.debug(
      {
        request_id: request.requestId,
        item: 'event',
        count,
        flushed,
      },
      'sentry.flush',
    );

    return {
      app: APP_NAME,
      kind: 'event',
      count,
      environment,
      request_id: request.requestId,
      event_ids: eventIds,
      flushed,
    };
  },
);

app.get(
  '/transactions',
  {
    schema: {
      querystring: querystringSchema,
      response: { 200: transactionResponseSchema },
    },
  },
  async (request) => {
    const count = request.query.count;
    const environment = request.query.environment || defaultEnvironment;
    const traceIds = [];

    request.log.debug(
      {
        request_id: request.requestId,
        count,
        environment,
      },
      'sample.transactions.requested',
    );

    for (let index = 0; index < count; index += 1) {
      Sentry.withScope((scope) => {
        scope.setTag('sample_app', APP_NAME);
        scope.setTag('sample_environment', environment);
        scope.setTag('sample_request_id', request.requestId);

        const span = Sentry.startInactiveSpan({
          name: `sample.transaction.${APP_NAME}`,
          op: 'sample.task',
          forceTransaction: true,
          parentSpan: null,
        });

        span.setAttribute('sample.app', APP_NAME);
        span.setAttribute('sample.environment_override', environment);
        span.setAttribute('sample.request_id', request.requestId);
        span.setAttribute('sample.event_index', index);
        span.end();

        const spanContext = span.spanContext();
        traceIds.push(spanContext.traceId);

        request.log.debug(
          {
            request_id: request.requestId,
            trace_id: spanContext.traceId,
            span_id: spanContext.spanId,
            event_index: index,
            environment,
          },
          'sentry.capture.transaction',
        );
      });
    }

    const flushed = await Sentry.flush(5000);
    request.log.debug(
      {
        request_id: request.requestId,
        item: 'transaction',
        count,
        flushed,
      },
      'sentry.flush',
    );

    return {
      app: APP_NAME,
      kind: 'transaction',
      count,
      environment,
      request_id: request.requestId,
      trace_ids: traceIds,
      flushed,
    };
  },
);

closeWithGrace({ delay: 10_000 }, async ({ signal, err }) => {
  if (err) {
    app.log.error({ err }, 'server.closing.error');
  } else {
    app.log.info({ signal }, 'server.closing.signal');
  }

  await app.close();
  const flushed = await Sentry.close(5000);
  logJson('info', 'app.stopped', { sentry_closed: flushed });
});

await app.listen({ host: '0.0.0.0', port });
logJson('info', 'app.started', { port });
