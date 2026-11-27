import { format } from 'node:util';

const APP_NAME = 'fastify';

function safeStringify(value) {
  const seen = new WeakSet();
  return JSON.stringify(value, (_key, item) => {
    if (typeof item === 'bigint') {
      return item.toString();
    }
    if (typeof item === 'object' && item !== null) {
      if (seen.has(item)) {
        return '[Circular]';
      }
      seen.add(item);
    }
    return item;
  });
}

function emit(level, message, fields = {}) {
  const payload = {
    timestamp: new Date().toISOString(),
    level,
    app: APP_NAME,
    logger: 'console',
    message,
    ...fields,
  };
  process.stdout.write(`${safeStringify(payload)}\n`);
}

/**
 * Routes all console output (including Sentry SDK debug logs) through the
 * JSON line formatter. Pino/Fastify logs are already JSON and do not use
 * console, so they are unaffected.
 */
export function installJsonConsole() {
  const methods = [
    ['log', 'debug'],
    ['info', 'info'],
    ['debug', 'debug'],
    ['trace', 'trace'],
    ['warn', 'warn'],
    ['error', 'error'],
  ];

  for (const [method, level] of methods) {
    console[method] = (...args) => emit(level, format(...args));
  }

  console.assert = (condition, ...args) => {
    if (!condition) {
      emit('error', format(...args));
    }
  };
}

export function logJson(level, message, fields = {}) {
  emit(level, message, fields);
}

export { APP_NAME };
