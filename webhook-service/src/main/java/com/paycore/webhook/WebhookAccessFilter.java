package com.paycore.webhook;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.UUID;
import com.paycore.common.security.ServiceTokenValidator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/** Enforces merchant ownership and independent operational credentials for webhook APIs. */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 5)
public class WebhookAccessFilter extends OncePerRequestFilter {

    static final String MERCHANT_ID_ATTR = "paycore.merchantId";
    private final JdbcClient jdbc;
    private final String adminToken;

    public WebhookAccessFilter(JdbcClient jdbc, @Value("${paycore.security.admin-token:}") String adminToken) {
        this.jdbc = jdbc;
        this.adminToken = adminToken == null ? "" : adminToken.trim();
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getRequestURI();
        return path == null || !path.startsWith("/v1/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String path = request.getRequestURI();
        if (path.startsWith("/v1/admin/")) {
            if (!matches(adminToken, request.getHeader("X-PayCore-Admin-Token"))) {
                response.setStatus(adminToken.isEmpty() ? 503 : 401);
                return;
            }
        } else if (path.startsWith("/v1/webhook-")) {
            String rawKey = request.getHeader("X-API-Key");
            if (rawKey == null || rawKey.isBlank()) {
                response.setStatus(401);
                return;
            }
            String hash = sha256Hex(rawKey.trim());
            var id = jdbc.sql("SELECT id FROM merchants WHERE api_key_hash = :hash")
                    .param("hash", hash)
                    .query((rs, rowNum) -> rs.getObject("id", UUID.class))
                    .optional();
            if (id.isEmpty()) {
                response.setStatus(401);
                return;
            }
            request.setAttribute(MERCHANT_ID_ATTR, id.get());
        }
        chain.doFilter(request, response);
    }

    private static boolean matches(String expected, String actual) {
        return ServiceTokenValidator.matches(expected, actual);
    }

    private static String sha256Hex(String raw) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(raw.getBytes(StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
