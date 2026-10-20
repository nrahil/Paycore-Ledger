package com.paycore.payments;

import static org.assertj.core.api.Assertions.assertThat;

import com.paycore.payments.psp.PspClient;
import com.paycore.payments.psp.ResilientPspClient;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PspOperationFailureTest {

    private HttpServer server;

    @BeforeEach
    void startFailingPsp() throws Exception {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(500, -1);
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void stopFailingPsp() {
        server.stop(0);
    }

    @Test
    void captureHttpFailureIsNotReportedAsSuccess() {
        ResilientPspClient client = new ResilientPspClient(pspUrl(), 500, "");

        PspClient.PspCaptureResult result = client.capture(new PspClient.PspCaptureRequest(
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), "psp-ref", 1000L, "INR"));

        assertThat(result.outcome()).isEqualTo(PspClient.PspOutcome.UNKNOWN_TIMEOUT);
    }

    @Test
    void refundHttpFailureIsNotReportedAsSuccess() {
        ResilientPspClient client = new ResilientPspClient(pspUrl(), 500, "");

        PspClient.PspRefundResult result = client.refund(new PspClient.PspRefundRequest(
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), "psp-ref", 100L, "INR"));

        assertThat(result.outcome()).isEqualTo(PspClient.PspOutcome.UNKNOWN_TIMEOUT);
    }

    private String pspUrl() {
        return "http://localhost:" + server.getAddress().getPort();
    }
}
