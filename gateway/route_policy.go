package main

import (
	"net/http"
	"net/http/httputil"
	"strings"
)

func (g *Gateway) selectUpstreamProxy(method, path string) *httputil.ReverseProxy {
	// CQRS Read Model routes
	if (method == http.MethodGet && path == "/v1/payments") ||
		path == "/v1/merchants/me/summary" ||
		strings.HasPrefix(path, "/v1/admin/read-model") {
		return g.readModelProxy
	}
	// Webhook Service routes
	if strings.HasPrefix(path, "/v1/webhook-") ||
		strings.HasPrefix(path, "/v1/admin/webhooks") ||
		strings.HasPrefix(path, "/v1/admin/dlq") ||
		strings.HasPrefix(path, "/v1/admin/consumers") {
		return g.webhookProxy
	}
	// Reconciliation Service routes
	if strings.HasPrefix(path, "/v1/admin/reconciliation") {
		return g.reconProxy
	}
	// Default: Authoritative Payments Core & Ledger Service
	return g.paymentsProxy
}

func requiresMerchantAuth(method, path string) bool {
	if path == "/v1/merchants" && method == http.MethodPost {
		return false
	}
	if strings.HasPrefix(path, "/v1/admin/") {
		return false
	}
	return strings.HasPrefix(path, "/v1/payments") ||
		strings.HasPrefix(path, "/v1/payouts") ||
		strings.HasPrefix(path, "/v1/merchants/me")
}

func requiresIdempotencyKey(method, path string) bool {
	if method != http.MethodPost {
		return false
	}
	return path == "/v1/payments" ||
		path == "/v1/payouts" ||
		(strings.HasPrefix(path, "/v1/payments/") &&
			(strings.HasSuffix(path, "/authorize") ||
				strings.HasSuffix(path, "/capture") ||
				strings.HasSuffix(path, "/void") ||
				strings.HasSuffix(path, "/refunds")))
}
