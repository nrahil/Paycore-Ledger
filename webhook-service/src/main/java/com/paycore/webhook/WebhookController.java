package com.paycore.webhook;

import com.paycore.common.errors.PayCoreException;
import jakarta.servlet.http.HttpServletRequest;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Webhook Delivery, DLQ, Replay, and Consumer Lag REST APIs (Section 5.6).
 */
@RestController
public class WebhookController {

    private final WebhookDeliveryService webhookService;
    private final WebhookReplayService replayService;
    private final WebhookQueryService queryService;
    private final JdbcClient jdbcClient;

    public WebhookController(
            WebhookDeliveryService webhookService,
            WebhookReplayService replayService,
            WebhookQueryService queryService,
            JdbcClient jdbcClient) {
        this.webhookService = webhookService;
        this.replayService = replayService;
        this.queryService = queryService;
        this.jdbcClient = jdbcClient;
    }

    @PostMapping("/v1/webhook-endpoints")
    public ResponseEntity<Map<String, String>> registerEndpoint(
            HttpServletRequest httpRequest, @RequestBody RegisterEndpointRequest request) {
        UUID authenticatedMerchantId = requireMerchantId(httpRequest);
        if (request.merchantId() != null && !request.merchantId().equals(authenticatedMerchantId)) {
            throw new org.springframework.web.server.ResponseStatusException(
                    HttpStatus.FORBIDDEN, "Cannot register a webhook endpoint for another merchant");
        }
        webhookService.registerMerchantEndpoint(
                authenticatedMerchantId, request.webhookUrl(), request.webhookSecret());
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("status", "REGISTERED"));
    }

    @GetMapping("/v1/webhook-deliveries")
    public ResponseEntity<List<WebhookViews.DeliveryRow>> listDeliveries(
            HttpServletRequest httpRequest,
            @RequestParam(value = "merchantId", required = false) UUID merchantId) {
        UUID authenticatedMerchantId = requireMerchantId(httpRequest);
        if (merchantId != null && !merchantId.equals(authenticatedMerchantId)) {
            throw new org.springframework.web.server.ResponseStatusException(
                    HttpStatus.FORBIDDEN, "Cannot access another merchant's deliveries");
        }
        return ResponseEntity.ok(queryService.listDeliveries(authenticatedMerchantId));
    }

    @PostMapping("/v1/webhook-deliveries/{id}/replay")
    public ResponseEntity<WebhookDeliveryService.DeliveryOutcome> replayDelivery(
            HttpServletRequest request,
            @PathVariable("id") UUID deliveryId) {
        return ResponseEntity.ok(replayService.replayDeliveryForMerchant(
                requireMerchantId(request), deliveryId));
    }

    @PostMapping("/v1/admin/webhooks/{id}/replay")
    public ResponseEntity<WebhookDeliveryService.DeliveryOutcome> replayDeliveryAsAdmin(
            @PathVariable("id") UUID deliveryId) {
        return ResponseEntity.ok(replayService.replayDelivery(deliveryId));
    }

    @GetMapping("/v1/admin/dlq")
    public ResponseEntity<List<WebhookViews.DlqEventRow>> listDlq() {
        return ResponseEntity.ok(queryService.listDlqEvents());
    }

    @PostMapping("/v1/admin/dlq/{id}/replay")
    public ResponseEntity<WebhookViews.DlqEventRow> replayDlq(
            @PathVariable("id") UUID dlqId) {
        return ResponseEntity.ok(replayService.replayDlqEvent(dlqId));
    }

    @GetMapping("/v1/admin/consumers/lag")
    public ResponseEntity<WebhookViews.ConsumerLagView> getConsumerLag() {
        return ResponseEntity.ok(queryService.getConsumerLagStats());
    }

    @GetMapping("/healthz")
    public ResponseEntity<Map<String, String>> healthz() {
        return ResponseEntity.ok(Map.of("status", "UP", "service", "webhook-service"));
    }

    @GetMapping("/readyz")
    public ResponseEntity<Map<String, String>> readyz() {
        jdbcClient.sql("SELECT 1").query(Integer.class).single();
        return ResponseEntity.ok(Map.of("status", "READY", "service", "webhook-service"));
    }

    @ExceptionHandler(PayCoreException.class)
    public ResponseEntity<ProblemDetail> handlePayCoreException(
            PayCoreException ex, HttpServletRequest request) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.valueOf(ex.httpStatus()), ex.getMessage());
        problem.setType(ex.typeUri());
        problem.setTitle(ex.title());
        problem.setInstance(URI.create(request.getRequestURI()));
        problem.setProperty("errorCode", ex.errorCode());
        return ResponseEntity.status(ex.httpStatus())
                .contentType(MediaType.APPLICATION_PROBLEM_JSON)
                .body(problem);
    }

    public record RegisterEndpointRequest(UUID merchantId, String webhookUrl, String webhookSecret) {}

    private UUID requireMerchantId(HttpServletRequest request) {
        UUID merchantId = (UUID) request.getAttribute(WebhookAccessFilter.MERCHANT_ID_ATTR);
        if (merchantId == null) {
            throw new org.springframework.web.server.ResponseStatusException(
                    HttpStatus.UNAUTHORIZED, "Authenticated merchant required");
        }
        return merchantId;
    }
}
