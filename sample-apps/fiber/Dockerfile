FROM golang:1.24.1-bookworm AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/fiber-app .

FROM alpine:3.21.3

RUN apk add --no-cache ca-certificates \
    && adduser -D -u 10001 appuser

COPY --from=build /out/fiber-app /usr/local/bin/fiber-app

USER appuser

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=5 \
    CMD wget -qO- http://127.0.0.1:8080/health >/dev/null 2>&1 || exit 1

ENTRYPOINT ["/usr/local/bin/fiber-app"]
