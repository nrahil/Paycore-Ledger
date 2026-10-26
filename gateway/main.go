package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// GatewayConfig holds upstream service URLs and edge protection limits.
type GatewayConfig struct {
	ListenAddr        string
	PaymentsCoreURL   string
	WebhookServiceURL string
	ReadModelURL      string
	ReconciliationURL string
	RateLimitRPS      float64
	RateLimitBurst    int
	MaxBodyBytes      int64
	UpstreamTimeout   time.Duration
}

// Gateway implements the PayCore Edge Gateway (Section 1.2 Stretch 4 & Section 4.1).
type Gateway struct {
	cfg             GatewayConfig
	rateLimiter     *rateLimiter
	paymentsProxy   *httputil.ReverseProxy
	webhookProxy    *httputil.ReverseProxy
	readModelProxy  *httputil.ReverseProxy
	reconProxy      *httputil.ReverseProxy
	totalRequests   atomic.Uint64
	rateLimitedReqs atomic.Uint64
}

type problemDetail struct {
	Type      string `json:"type"`
	Title     string `json:"title"`
	Status    int    `json:"status"`
	Detail    string `json:"detail"`
	Instance  string `json:"instance"`
	ErrorCode string `json:"errorCode"`
	Timestamp string `json:"timestamp"`
}

func NewGateway(cfg GatewayConfig) (*Gateway, error) {
	if cfg.RateLimitRPS <= 0 {
		cfg.RateLimitRPS = 100
	}
	if cfg.RateLimitBurst <= 0 {
		cfg.RateLimitBurst = 200
	}
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = 1024 * 1024 // 1 MiB
	}
	if cfg.UpstreamTimeout <= 0 {
		cfg.UpstreamTimeout = 10 * time.Second
	}

	paymentsProxy, err := newReverseProxy(cfg.PaymentsCoreURL, cfg.UpstreamTimeout)
	if err != nil {
		return nil, fmt.Errorf("invalid payments-core URL: %w", err)
	}
	webhookProxy, err := newReverseProxy(cfg.WebhookServiceURL, cfg.UpstreamTimeout)
	if err != nil {
		return nil, fmt.Errorf("invalid webhook-service URL: %w", err)
	}
	readModelProxy, err := newReverseProxy(cfg.ReadModelURL, cfg.UpstreamTimeout)
	if err != nil {
		return nil, fmt.Errorf("invalid read-model-service URL: %w", err)
	}
	reconProxy, err := newReverseProxy(cfg.ReconciliationURL, cfg.UpstreamTimeout)
	if err != nil {
		return nil, fmt.Errorf("invalid reconciliation URL: %w", err)
	}

	gw := &Gateway{
		cfg:            cfg,
		paymentsProxy:  paymentsProxy,
		webhookProxy:   webhookProxy,
		readModelProxy: readModelProxy,
		reconProxy:     reconProxy,
		rateLimiter:    newRateLimiter(cfg.RateLimitRPS, cfg.RateLimitBurst),
	}
	go gw.startBucketEvictionLoop(5 * time.Minute)
	return gw, nil
}

func (g *Gateway) startBucketEvictionLoop(interval time.Duration) {
	g.rateLimiter.startBucketEvictionLoop(interval)
}

func (g *Gateway) evictStaleBuckets(now time.Time) int {
	return g.rateLimiter.evictStaleBuckets(now)
}

func newReverseProxy(rawURL string, timeout time.Duration) (*httputil.ReverseProxy, error) {
	target, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if (target.Scheme != "http" && target.Scheme != "https") || target.Host == "" {
		return nil, fmt.Errorf("upstream URL must have an http(s) scheme and host")
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = &http.Transport{
		ResponseHeaderTimeout: timeout,
		MaxIdleConns:          512,
		MaxIdleConnsPerHost:   256,
		IdleConnTimeout:       90 * time.Second,
	}
	return proxy, nil
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.totalRequests.Add(1)
	path := r.URL.Path

	// 1. Gateway health, readiness & Prometheus metrics endpoints
	if path == "/healthz" || path == "/readyz" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"UP","service":"paycore-go-gateway"}`))
		return
	}
	if path == "/metrics" {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w,
			"# HELP paycore_gateway_requests_total Total HTTP requests handled by Go gateway\n"+
				"# TYPE paycore_gateway_requests_total counter\n"+
				"paycore_gateway_requests_total %d\n"+
				"# HELP paycore_gateway_rate_limited_total Total HTTP requests rejected with 429 by Go gateway\n"+
				"# TYPE paycore_gateway_rate_limited_total counter\n"+
				"paycore_gateway_rate_limited_total %d\n",
			g.totalRequests.Load(),
			g.rateLimitedReqs.Load())
		return
	}

	// 2. Ensure W3C traceparent header exists and propagate to upstream & response.
	// The production handler is wrapped by otelhttp, so this injects the active
	// gateway span. Direct Gateway tests retain a valid standalone trace context.
	traceparent := strings.TrimSpace(r.Header.Get("traceparent"))
	if oteltrace.SpanFromContext(r.Context()).SpanContext().IsValid() {
		otel.GetTextMapPropagator().Inject(r.Context(), propagation.HeaderCarrier(r.Header))
		traceparent = r.Header.Get("traceparent")
	} else if traceparent == "" {
		traceparent = generateTraceparent()
		r.Header.Set("traceparent", traceparent)
	}
	w.Header().Set("traceparent", traceparent)
	w.Header().Set("X-Trace-Id", extractTraceID(traceparent))

	// 3. Enforce payload size limit at the edge
	if r.ContentLength > g.cfg.MaxBodyBytes {
		writeProblem(w, r, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE",
			"Request Payload Too Large",
			fmt.Sprintf("Request body exceeds maximum allowed size of %d bytes", g.cfg.MaxBodyBytes))
		return
	}
	if r.Body != nil {
		r.Body = http.MaxBytesReader(w, r.Body, g.cfg.MaxBodyBytes)
	}

	// 4. Enforce X-API-Key authentication on merchant-scoped routes
	apiKey := strings.TrimSpace(r.Header.Get("X-API-Key"))
	if requiresMerchantAuth(r.Method, path) && apiKey == "" {
		writeProblem(w, r, http.StatusUnauthorized, "MISSING_API_KEY",
			"Unauthorized",
			"Missing required X-API-Key header at edge gateway")
		return
	}

	// 5. Per-merchant (or per-IP) token-bucket rate limiting
	rateKey := apiKey
	if rateKey == "" {
		rateKey = r.RemoteAddr
	}
	allowed, remaining, retryAfterSec := g.rateLimiter.allow(rateKey)
	w.Header().Set("X-RateLimit-Limit", strconv.Itoa(g.cfg.RateLimitBurst))
	w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(remaining))
	if !allowed {
		g.rateLimitedReqs.Add(1)
		w.Header().Set("Retry-After", strconv.Itoa(retryAfterSec))
		writeProblem(w, r, http.StatusTooManyRequests, "RATE_LIMIT_EXCEEDED",
			"Too Many Requests",
			"Merchant token-bucket rate limit exceeded; back off and retry after Retry-After seconds")
		return
	}

	// 6. Enforce Idempotency-Key (or X-Idempotency-Key) on state-changing money mutations at the edge
	if requiresIdempotencyKey(r.Method, path) {
		idemKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
		if idemKey == "" {
			idemKey = strings.TrimSpace(r.Header.Get("X-Idempotency-Key"))
			if idemKey != "" {
				r.Header.Set("Idempotency-Key", idemKey)
			}
		}
		if idemKey == "" {
			writeProblem(w, r, http.StatusBadRequest, "MISSING_IDEMPOTENCY_KEY",
				"Missing Idempotency-Key Header",
				"All state-changing payment and payout requests must include Idempotency-Key")
			return
		}
	}

	// 7. Route to appropriate backend microservice (CQRS split + domain routing)
	targetProxy := g.selectUpstreamProxy(r.Method, path)
	ctx, cancel := context.WithTimeout(r.Context(), g.cfg.UpstreamTimeout)
	defer cancel()
	targetProxy.ServeHTTP(w, r.WithContext(ctx))
}

func generateTraceparent() string {
	var traceID [16]byte
	var spanID [8]byte
	_, _ = io.ReadFull(rand.Reader, traceID[:])
	_, _ = io.ReadFull(rand.Reader, spanID[:])
	return fmt.Sprintf("00-%s-%s-01", hex.EncodeToString(traceID[:]), hex.EncodeToString(spanID[:]))
}

func extractTraceID(traceparent string) string {
	parts := strings.Split(traceparent, "-")
	if len(parts) >= 2 && len(parts[1]) == 32 {
		return parts[1]
	}
	return traceparent
}

func writeProblem(w http.ResponseWriter, r *http.Request, status int, code, title, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	body, _ := json.Marshal(problemDetail{
		Type:      "https://api.paycore.internal/errors/" + strings.ToLower(strings.ReplaceAll(code, "_", "-")),
		Title:     title,
		Status:    status,
		Detail:    detail,
		Instance:  r.URL.Path,
		ErrorCode: code,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	})
	_, _ = w.Write(body)
}

func envOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func main() {
	shutdownTracing, err := initTracing()
	if err != nil {
		log.Fatalf("Failed to initialize OpenTelemetry tracing: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTracing(ctx); err != nil {
			log.Printf("Failed to flush OpenTelemetry traces: %v", err)
		}
	}()

	rps, _ := strconv.ParseFloat(envOrDefault("RATE_LIMIT_RPS", "200"), 64)
	burst, _ := strconv.Atoi(envOrDefault("RATE_LIMIT_BURST", "400"))

	cfg := GatewayConfig{
		ListenAddr:        envOrDefault("GATEWAY_LISTEN_ADDR", ":8090"),
		PaymentsCoreURL:   envOrDefault("PAYMENTS_CORE_URL", "http://localhost:8080"),
		WebhookServiceURL: envOrDefault("WEBHOOK_SERVICE_URL", "http://localhost:8082"),
		ReadModelURL:      envOrDefault("READ_MODEL_URL", "http://localhost:8083"),
		ReconciliationURL: envOrDefault("RECONCILIATION_URL", "http://localhost:8084"),
		RateLimitRPS:      rps,
		RateLimitBurst:    burst,
		MaxBodyBytes:      1024 * 1024,
		UpstreamTimeout:   10 * time.Second,
	}

	gw, err := NewGateway(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize PayCore Go Edge Gateway: %v", err)
	}

	log.Printf("PayCore Go Edge Gateway listening on %s", cfg.ListenAddr)
	handler := otelhttp.NewHandler(gw, "paycore-gateway")
	server := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Gateway server exited: %v", err)
	}
}

func initTracing() (func(context.Context) error, error) {
	exporter, err := otlptracegrpc.New(context.Background(), otlptracegrpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	res := resource.NewWithAttributes("", attribute.String("service.name", "paycore-gateway"))
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	return provider.Shutdown, nil
}
