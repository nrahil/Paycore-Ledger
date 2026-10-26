package com.paycore.webhook;

import com.paycore.common.clock.ClockProvider;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/** Coordinates due-retry claims, delivery attempts, and lease-token guarded finalization. */
@Service
public class WebhookRetryCoordinator {

    private static final Logger log = LoggerFactory.getLogger(WebhookRetryCoordinator.class);

    private final ClockProvider clockProvider;
    private final WebhookRetryRepository retryRepository;
    private final WebhookDeliveryService deliveryService;
    private final int deliveryLeaseSeconds;

    public WebhookRetryCoordinator(
            ClockProvider clockProvider,
            WebhookRetryRepository retryRepository,
            WebhookDeliveryService deliveryService,
            @Value("${paycore.webhook.delivery-lease-seconds:30}") int deliveryLeaseSeconds) {
        this.clockProvider = clockProvider;
        this.retryRepository = retryRepository;
        this.deliveryService = deliveryService;
        this.deliveryLeaseSeconds = Math.max(1, deliveryLeaseSeconds);
    }

    @Scheduled(fixedDelayString = "${paycore.webhook.retry-poll-interval-ms:1000}")
    public void scheduledRetryTick() {
        try {
            processDueRetries(clockProvider.now());
        } catch (Exception failure) {
            log.warn("Webhook retry worker failed and will retry on the next poll: {}", failure.getMessage());
        }
    }

    /** Processes a bounded batch; outbound delivery runs after the claim transaction has committed. */
    public int processDueRetries(Instant asOf) {
        List<WebhookRetryRepository.RetryClaim> claims = retryRepository.claimDue(asOf, deliveryLeaseSeconds);
        int attempted = 0;
        for (WebhookRetryRepository.RetryClaim claim : claims) {
            try {
                WebhookDeliveryService.RetryProcessingResult result = deliveryService.processRetryClaim(claim);
                if (result.attempted()) attempted++;
                retryRepository.finalizeClaim(claim.id(), claim.claimToken(), "FAILED", result.error());
            } catch (RuntimeException failure) {
                retryRepository.finalizeClaim(claim.id(), claim.claimToken(), "FAILED", null);
                throw failure;
            }
        }
        return attempted;
    }
}
