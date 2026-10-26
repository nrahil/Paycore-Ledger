package com.paycore.readmodel;

import java.util.Map;
import java.util.UUID;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST endpoints for the CQRS Read Model Service (Section 6.1):
 * <ul>
 *   <li>{@code GET /v1/payments?cursor=&limit=} (paginated by UUIDv7 cursor)</li>
 *   <li>{@code GET /v1/merchants/me/summary}</li>
 *   <li>{@code POST /v1/admin/read-model/rebuild}</li>
 *   <li>{@code GET /v1/admin/read-model/dead-letters} and {@code POST .../{id}/replay}</li>
 *   <li>{@code POST /v1/internal/read-model/ingest} (direct testing/relay hook)</li>
 * </ul>
 */
@RestController
public class ReadModelController {

    private final ReadModelProjectorService projectorService;
    private final ReadModelQueryService queryService;
    private final ReadModelRebuildService rebuildService;
    private final ReadModelDeadLetterService deadLetterService;
    private final JdbcClient jdbcClient;

    public ReadModelController(
            ReadModelProjectorService projectorService,
            ReadModelQueryService queryService,
            ReadModelRebuildService rebuildService,
            ReadModelDeadLetterService deadLetterService,
            JdbcClient jdbcClient) {
        this.projectorService = projectorService;
        this.queryService = queryService;
        this.rebuildService = rebuildService;
        this.deadLetterService = deadLetterService;
        this.jdbcClient = jdbcClient;
    }

    @GetMapping("/v1/payments")
    public ResponseEntity<ReadModelViews.CursorPage<ReadModelViews.PaymentViewRow>> listPayments(
            HttpServletRequest request,
            @RequestParam(value = "cursor", required = false) UUID cursor,
            @RequestParam(value = "limit", defaultValue = "20") int limit) {
        UUID merchantId = authenticatedMerchantId(request);
        return ResponseEntity.ok(queryService.listPayments(merchantId, cursor, limit));
    }

    @GetMapping("/v1/merchants/me/summary")
    public ResponseEntity<ReadModelViews.MerchantSummaryResponse> getMerchantSummary(
            HttpServletRequest request) {
        UUID merchantId = authenticatedMerchantId(request);
        return ResponseEntity.ok(queryService.getMerchantSummary(merchantId));
    }

    @PostMapping("/v1/admin/read-model/rebuild")
    public ResponseEntity<ReadModelViews.RebuildReport> rebuildReadModel() {
        return ResponseEntity.ok(rebuildService.rebuildFromScratch());
    }

    @GetMapping("/v1/admin/read-model/dead-letters")
    public ResponseEntity<java.util.List<ReadModelViews.DeadLetterView>> listDeadLetters(
            @RequestParam(value = "limit", defaultValue = "50") int limit) {
        return ResponseEntity.ok(deadLetterService.listDeadLetters(limit));
    }

    @PostMapping("/v1/admin/read-model/dead-letters/{id}/replay")
    public ResponseEntity<ReadModelViews.DeadLetterReplayResult> replayDeadLetter(
            @PathVariable UUID id) {
        return ResponseEntity.accepted().body(deadLetterService.replay(id));
    }

    @PostMapping("/v1/internal/read-model/ingest")
    public ResponseEntity<Map<String, Object>> ingestRawEvent(@RequestBody byte[] rawPayload) {
        projectorService.ingestEvent(rawPayload);
        return ResponseEntity.ok(Map.of("status", "ingested"));
    }

    @GetMapping("/healthz")
    public ResponseEntity<Map<String, String>> healthz() {
        return ResponseEntity.ok(Map.of("status", "UP", "service", "read-model-service"));
    }

    @GetMapping("/readyz")
    public ResponseEntity<Map<String, String>> readyz() {
        jdbcClient.sql("SELECT 1").query(Integer.class).single();
        return ResponseEntity.ok(Map.of("status", "READY", "service", "read-model-service"));
    }

    private UUID authenticatedMerchantId(HttpServletRequest request) {
        UUID merchantId = (UUID) request.getAttribute(ReadModelAccessFilter.MERCHANT_ID_ATTR);
        if (merchantId == null) {
            throw new org.springframework.web.server.ResponseStatusException(
                    org.springframework.http.HttpStatus.UNAUTHORIZED, "Authenticated merchant required");
        }
        return merchantId;
    }
}
