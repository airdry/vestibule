// Fiber sample application used to exercise vestibule.
//
// It intentionally uses the Sentry Go SDK version that was current for Sentry
// 25.3 (released 2025-03-17) and exposes two easy-to-call endpoints:
//
//	GET /events       - captures error events
//	GET /transactions - finishes manually forced transactions
//
// Both endpoints accept `count` and `environment` query parameters. Every log
// line, including Sentry SDK debug output, is emitted as JSON to stdout.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/gofiber/fiber/v2"
)

const (
	appName            = "fiber"
	defaultDSN         = "http://public@host.docker.internal:8080/3"
	defaultEnvironment = "production"
	defaultRelease     = "sample-apps@local"
	traceLevel         = slog.Level(-8)
)

var logger = newJSONLogger()

func main() {
	if len(os.Args) > 1 && os.Args[1] == "-healthcheck" {
		os.Exit(healthcheck())
	}

	dsn := envOrDefault("SENTRY_DSN", defaultDSN)
	environment := envOrDefault("SENTRY_ENVIRONMENT", defaultEnvironment)
	release := envOrDefault("SENTRY_RELEASE", defaultRelease)
	port := envOrDefault("PORT", "8080")

	initSentry(dsn, environment, release)

	app := fiber.New(fiber.Config{
		AppName:               "vestibule Fiber sample",
		DisableStartupMessage: true,
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			logger.Error(
				"http.request.failed",
				"request_id", getRequestID(c),
				"error", err.Error(),
			)
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": err.Error(),
			})
		},
	})

	app.Use(requestLogging)

	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"status": "ok", "app": appName})
	})

	app.Get("/events", func(c *fiber.Ctx) error {
		reqID := getRequestID(c)
		count := parseCount(c.Query("count", "1"))
		targetEnvironment := c.Query("environment", environment)
		eventIDs := make([]string, 0, count)

		logger.Debug(
			"sample.events.requested",
			"request_id", reqID,
			"count", count,
			"environment", targetEnvironment,
		)

		for index := range count {
			var eventID *sentry.EventID

			sentry.WithScope(func(scope *sentry.Scope) {
				scope.SetTag("sample_app", appName)
				scope.SetTag("sample_environment", targetEnvironment)
				scope.SetTag("sample_request_id", reqID)
				scope.SetExtra("event_index", index)

				eventID = sentry.CaptureException(fmt.Errorf(
					"sample %s event %d request_id=%q",
					appName,
					index,
					reqID,
				))
			})

			eventIDString := ""
			if eventID != nil {
				eventIDString = string(*eventID)
			}
			eventIDs = append(eventIDs, eventIDString)

			logger.Debug(
				"sentry.capture.event",
				"request_id", reqID,
				"event_id", eventIDString,
				"event_index", index,
				"environment", targetEnvironment,
			)
		}

		flushed := sentry.Flush(5 * time.Second)
		logger.Debug(
			"sentry.flush",
			"request_id", reqID,
			"item", "event",
			"count", count,
			"flushed", flushed,
		)

		return c.JSON(fiber.Map{
			"app":         appName,
			"kind":        "event",
			"count":       count,
			"environment": targetEnvironment,
			"request_id":  reqID,
			"event_ids":   eventIDs,
			"flushed":     flushed,
		})
	})

	app.Get("/transactions", func(c *fiber.Ctx) error {
		reqID := getRequestID(c)
		count := parseCount(c.Query("count", "1"))
		targetEnvironment := c.Query("environment", environment)
		traceIDs := make([]string, 0, count)

		logger.Debug(
			"sample.transactions.requested",
			"request_id", reqID,
			"count", count,
			"environment", targetEnvironment,
		)

		for index := range count {
			transaction := sentry.StartTransaction(
				context.Background(),
				fmt.Sprintf("sample.transaction.%s", appName),
				sentry.WithOpName("sample.task"),
				sentry.WithSpanSampled(sentry.SampledTrue),
			)
			transaction.SetTag("sample_app", appName)
			transaction.SetTag("sample_environment", targetEnvironment)
			transaction.SetTag("sample_request_id", reqID)
			transaction.SetData("event_index", index)

			time.Sleep(time.Millisecond)
			transaction.Finish()

			traceID := transaction.TraceID.String()
			traceIDs = append(traceIDs, traceID)

			logger.Debug(
				"sentry.capture.transaction",
				"request_id", reqID,
				"trace_id", traceID,
				"span_id", transaction.SpanID.String(),
				"event_index", index,
				"environment", targetEnvironment,
				"sampled", transaction.Sampled.Bool(),
			)
		}

		flushed := sentry.Flush(5 * time.Second)
		logger.Debug(
			"sentry.flush",
			"request_id", reqID,
			"item", "transaction",
			"count", count,
			"flushed", flushed,
		)

		return c.JSON(fiber.Map{
			"app":         appName,
			"kind":        "transaction",
			"count":       count,
			"environment": targetEnvironment,
			"request_id":  reqID,
			"trace_ids":   traceIDs,
			"flushed":     flushed,
		})
	})

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-shutdown
		logger.Info("app.stopping", "signal", sig.String())
		_ = app.Shutdown()
	}()

	logger.Info("app.started", "port", port)
	if err := app.Listen(":" + port); err != nil {
		logger.Error("app.listen.failed", "error", err.Error())
		os.Exit(1)
	}

	flushed := sentry.Flush(5 * time.Second)
	logger.Info("app.stopped", "sentry_flushed", flushed)
}

// healthcheck probes the local /health endpoint and returns a process exit
// code. The distroless runtime image has no shell or HTTP client, so the
// container HEALTHCHECK invokes the binary itself with -healthcheck.
func healthcheck() int {
	client := http.Client{Timeout: 2 * time.Second}
	response, err := client.Get(
		"http://127.0.0.1:" + envOrDefault("PORT", "8080") + "/health",
	)
	if err != nil {
		return 1
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

func newJSONLogger() *slog.Logger {
	level := parseLogLevel(envOrDefault("LOG_LEVEL", "debug"))
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
			switch attr.Key {
			case slog.TimeKey:
				attr.Key = "timestamp"
			case slog.MessageKey:
				attr.Key = "message"
			case slog.LevelKey:
				value, ok := attr.Value.Any().(slog.Level)
				if ok {
					if value == traceLevel {
						attr.Value = slog.StringValue("trace")
					} else {
						attr.Value = slog.StringValue(
							strings.ToLower(value.String()),
						)
					}
				}
			}
			return attr
		},
	})
	return slog.New(handler).With("app", appName)
}

func parseLogLevel(raw string) slog.Level {
	switch strings.ToLower(raw) {
	case "trace":
		return traceLevel
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelDebug
	}
}

func initSentry(dsn, environment, release string) {
	writer := sentryLogWriter{logger: logger}
	sentry.Logger = log.New(writer, "", 0)

	err := sentry.Init(sentry.ClientOptions{
		Dsn:              dsn,
		Environment:      environment,
		Release:          release,
		Debug:            true,
		DebugWriter:      writer,
		AttachStacktrace: true,
		// EnableTracing is required for manual transactions, while the zero
		// sample rate drops anything that is not explicitly forced to sample.
		EnableTracing:    true,
		TracesSampleRate: 0.0,
		BeforeSend: func(event *sentry.Event, _ *sentry.EventHint) *sentry.Event {
			applyEnvironmentOverride(event)
			logSentryPayload("event", event)
			return event
		},
		BeforeSendTransaction: func(
			event *sentry.Event,
			_ *sentry.EventHint,
		) *sentry.Event {
			applyEnvironmentOverride(event)
			logSentryPayload("transaction", event)
			return event
		},
	})
	if err != nil {
		logger.Error("sentry.init.failed", "error", err.Error())
		os.Exit(1)
	}

	logger.Info(
		"sentry.initialized",
		"dsn", dsn,
		"environment", environment,
		"release", release,
	)
}

func applyEnvironmentOverride(event *sentry.Event) {
	if environment, ok := event.Tags["sample_environment"]; ok {
		event.Environment = environment
	}
}

func logSentryPayload(item string, event *sentry.Event) {
	payload := *event
	// The installed-modules map is large and not useful for traffic checks.
	payload.Modules = nil

	raw, err := json.Marshal(&payload)
	if err != nil {
		logger.Debug(
			"sentry.payload",
			"sentry_item", item,
			"error", err.Error(),
		)
		return
	}

	logger.Debug(
		"sentry.payload",
		"sentry_item", item,
		"event_id", event.EventID,
		"payload", json.RawMessage(raw),
	)
}

type sentryLogWriter struct {
	logger *slog.Logger
}

func (w sentryLogWriter) Write(p []byte) (int, error) {
	message := strings.TrimSpace(string(p))
	if message != "" {
		w.logger.Debug("sentry.sdk", "detail", message)
	}
	return len(p), nil
}

func requestLogging(c *fiber.Ctx) error {
	id := c.Get("X-Request-Id")
	if id == "" {
		id = randomID()
	}
	c.Locals("request_id", id)
	c.Set("X-Request-Id", id)

	started := time.Now()
	logger.Debug(
		"http.request.started",
		"request_id", id,
		"method", c.Method(),
		"path", c.Path(),
		"query", string(c.Request().URI().QueryString()),
	)

	err := c.Next()

	logger.Debug(
		"http.request.finished",
		"request_id", id,
		"method", c.Method(),
		"path", c.Path(),
		"status", c.Response().StatusCode(),
		"duration_ms", float64(time.Since(started).Microseconds())/1000.0,
	)
	return err
}

func getRequestID(c *fiber.Ctx) string {
	id, ok := c.Locals("request_id").(string)
	if !ok {
		return ""
	}
	return id
}

func parseCount(raw string) int {
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 1
	}
	if value < 1 {
		return 1
	}
	if value > 1000 {
		return 1000
	}
	return value
}

func randomID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf[:])
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
