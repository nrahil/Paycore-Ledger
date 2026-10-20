package com.paycore.webhook;

import com.paycore.common.tracing.TraceHeaders;
import java.util.UUID;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

/** Owns the outbound HTTP and signing boundary for webhook deliveries. */
final class WebhookHttpSender {

    private final RestTemplate restTemplate;

    WebhookHttpSender(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    DeliveryResult send(
            String targetUrl,
            String webhookSecret,
            UUID eventId,
            String timestamp,
            String payloadJson,
            String traceparent,
            boolean allowLocalTargets) {
        try {
            WebhookSsrfValidator.validateAtDeliveryTime(targetUrl, allowLocalTargets);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.set(TraceHeaders.WEBHOOK_SIGNATURE, WebhookSigner.sign(webhookSecret, timestamp, payloadJson));
            headers.set(TraceHeaders.WEBHOOK_TIMESTAMP, timestamp);
            headers.set(TraceHeaders.WEBHOOK_EVENT_ID, eventId.toString());
            if (traceparent != null && !traceparent.isBlank()) {
                headers.set(TraceHeaders.TRACEPARENT, traceparent);
            }

            ResponseEntity<String> response = restTemplate.exchange(
                    targetUrl, HttpMethod.POST, new HttpEntity<>(payloadJson, headers), String.class);
            int statusCode = response.getStatusCode().value();
            return response.getStatusCode().is2xxSuccessful()
                    ? new DeliveryResult(true, statusCode, null)
                    : new DeliveryResult(false, statusCode, "HTTP " + statusCode);
        } catch (HttpStatusCodeException ex) {
            int statusCode = ex.getStatusCode().value();
            return new DeliveryResult(false, statusCode,
                    "HTTP " + statusCode + ": " + ex.getResponseBodyAsString());
        } catch (Exception ex) {
            return new DeliveryResult(false, null, ex.getMessage());
        }
    }

    record DeliveryResult(boolean delivered, Integer statusCode, String errorMessage) {}
}
