package com.paycore.mockpsp;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"paycore.security.internal-token=mock-psp-test-internal",
                "paycore.security.admin-token=mock-psp-test-admin"})
class MockPspControllerTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void authorizesIdempotentlyByPspRefAndExportsSettlementCsvWithDiscrepancies() {
        ResponseEntity<String> unauthenticated = restTemplate.postForEntity(
                "/psp/v1/authorizations",
                new MockPspController.AuthRequest("pay-unauth", "merch-1", "pay-unauth:1", 100L, "INR"),
                String.class);
        assertThat(unauthenticated.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);

        MockPspController.AuthRequest req1 =
                new MockPspController.AuthRequest("pay-1", "merch-1", "pay-1:1", 1000L, "INR");
        MockPspController.AuthRequest req2 =
                new MockPspController.AuthRequest("pay-2", "merch-1", "pay-2:1", 2000L, "INR");

        ResponseEntity<MockPspController.AuthResponse> resp1 = postInternal(
                "/psp/v1/authorizations", req1, MockPspController.AuthResponse.class);
        ResponseEntity<MockPspController.AuthResponse> resp2 = postInternal(
                "/psp/v1/authorizations", req2, MockPspController.AuthResponse.class);

        assertThat(resp1.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(resp1.getBody().status()).isEqualTo("AUTHORIZED");
        assertThat(resp2.getStatusCode()).isEqualTo(HttpStatus.OK);

        // Lookup by pspRef returns AUTHORIZED
        ResponseEntity<MockPspController.LookupResponse> lookup = getInternal(
                "/psp/v1/authorizations/pay-1:1", MockPspController.LookupResponse.class);
        assertThat(lookup.getBody().status()).isEqualTo("AUTHORIZED");
        assertThat(lookup.getBody().amountMinor()).isEqualTo(1000L);

        // Capture and partial refund on PSP
        ResponseEntity<MockPspController.AuthResponse> capResp = postInternal(
                "/psp/v1/captures/pay-1:1",
                new MockPspController.CaptureRequest("pay-1", 1000L, "INR", "capture-op-1"),
                MockPspController.AuthResponse.class);
        assertThat(capResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(capResp.getBody().status()).isEqualTo("CAPTURED");

        ResponseEntity<MockPspController.AuthResponse> refResp = postInternal(
                "/psp/v1/refunds/pay-1:1",
                new MockPspController.RefundPspRequest("pay-1", 300L, "INR", "refund-op-1"),
                MockPspController.AuthResponse.class);
        assertThat(refResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(refResp.getBody().amountMinor()).isEqualTo(700L);
        ResponseEntity<MockPspController.AuthResponse> retriedRefund = postInternal(
                "/psp/v1/refunds/pay-1:1",
                new MockPspController.RefundPspRequest("pay-1", 300L, "INR", "refund-op-1"),
                MockPspController.AuthResponse.class);
        assertThat(retriedRefund.getBody().amountMinor()).isEqualTo(700L);

        // Settlement CSV with injected discrepancies
        ResponseEntity<String> csv = getInternal("/psp/v1/settlement.csv?injectDiscrepancies=true", String.class);
        assertThat(csv.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(csv.getBody()).contains("psp_ref,amount_paise,status,settled_at,currency");
        assertThat(csv.getBody()).contains("pay-1:1").doesNotContain("pay-2:1");
        assertThat(csv.getBody()).contains("phantom-psp-ref:1");
    }

    private <T> ResponseEntity<T> postInternal(String path, Object body, Class<T> type) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-PayCore-Internal-Token", "mock-psp-test-internal");
        return restTemplate.exchange(path, HttpMethod.POST, new HttpEntity<>(body, headers), type);
    }

    private <T> ResponseEntity<T> getInternal(String path, Class<T> type) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-PayCore-Internal-Token", "mock-psp-test-internal");
        return restTemplate.exchange(path, HttpMethod.GET, new HttpEntity<>(headers), type);
    }
}
