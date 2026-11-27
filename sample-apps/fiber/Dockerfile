# Fiber sample application.
#
# The binary is built with CGO disabled so the runtime stage can use the fully
# static distroless image, which contains no shell, package manager, or HTTP
# client.

# Override with the :debug-nonroot variant to get a BusyBox shell for debugging.
ARG RUNTIME_IMAGE=gcr.io/distroless/static-debian13:nonroot

FROM golang:1.24.1-bookworm AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/fiber-app .

FROM ${RUNTIME_IMAGE}

COPY --from=build --chown=65532:65532 /out/fiber-app /usr/local/bin/fiber-app

USER nonroot:nonroot

EXPOSE 8080

# The distroless image has no shell and no curl, so the binary probes its own
# /health endpoint through the -healthcheck flag.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=5 \
    CMD ["/usr/local/bin/fiber-app", "-healthcheck"]

ENTRYPOINT ["/usr/local/bin/fiber-app"]
