package com.paycore.readmodel;

import com.paycore.readmodel.ReadModelProjectorService.CursorPage;
import com.paycore.readmodel.ReadModelProjectorService.MerchantSummaryResponse;
import com.paycore.readmodel.ReadModelProjectorService.PaymentViewRow;
import com.paycore.readmodel.ReadModelProjectorService.RebuildReport;
import java.util.Map;
import java.util.UUID;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST endpoints for the CQRS Read Model Service (Section 6.1):
 * <ul>
 *   <li>{@code GET /v1/payments?cursor=&limit=} (paginated by UUIDv7 cursor)</li>
 *   <li>{@code GET /v1/merchants/me/summary}</li>
 *   <li>{@code POST /v1/admin/read-model/rebuild}</li>
 *   <li>{@code POST /v1/internal/read-model/ingest} (direct testing/relay hook)</li>
 * </ul>
 */
@RestController
public class ReadModelController {

    private final ReadModelProjectorService projectorService;
    private final JdbcClient jdbcClient;

    public ReadModelController(ReadModelProjectorService projectorService, JdbcClient jdbcClient) {
        this.projectorService = projectorService;
        this.jdbcClient = jdbcClient;
    }

    @GetMapping("/v1/payments")
    public ResponseEntity<CursorPage<PaymentViewRow>> listPayments(
            HttpServletRequest request,
            @RequestParam(value = "cursor", required = false) UUID cursor,
            @RequestParam(value = "limit", defaultValue = "20") int limit) {
        UUID merchantId = authenticatedMerchantId(request);
        return ResponseEntity.ok(projectorService.listPayments(merchantId, cursor, limit));
    }

    @GetMapping("/v1/merchants/me/summary")
    public ResponseEntity<MerchantSummaryResponse> getMerchantSummary(
            HttpServletRequest request) {
        UUID merchantId = authenticatedMerchantId(request);
        return ResponseEntity.ok(projectorService.getMerchantSummary(merchantId));
    }

    @PostMapping("/v1/admin/read-model/rebuild")
    public ResponseEntity<RebuildReport> rebuildReadModel() {
        return ResponseEntity.ok(projectorService.rebuildFromScratch());
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
