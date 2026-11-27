# Fastify sample application.
#
# The build stage installs the production dependencies; the distroless runtime
# image ships Node.js 22 and glibc but no shell, package manager, or npm.

# Override with the :debug-nonroot variant to get a BusyBox shell for debugging.
ARG RUNTIME_IMAGE=gcr.io/distroless/nodejs22-debian13:nonroot

FROM node:22.23.5-bookworm-slim AS deps

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts

FROM ${RUNTIME_IMAGE}

ENV NODE_ENV=production

WORKDIR /app

COPY --from=deps --chown=65532:65532 /app/node_modules ./node_modules
COPY --chown=65532:65532 package.json ./
COPY --chown=65532:65532 src ./src

USER nonroot:nonroot

EXPOSE 3000

# The distroless image has no shell and no curl, so the probe runs the bundled
# Node.js runtime directly.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=5 \
    CMD ["/nodejs/bin/node", "-e", "fetch('http://127.0.0.1:3000/health').then((r) => { if (!r.ok) process.exit(1); }).catch(() => process.exit(1))"]

ENTRYPOINT ["/nodejs/bin/node"]
CMD ["src/server.js"]
