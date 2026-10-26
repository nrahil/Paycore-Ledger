package com.paycore.readmodel;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import com.paycore.common.security.ServiceTokenValidator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/** Authenticates merchant reads and separates trusted admin/internal operations. */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 5)
public class ReadModelAccessFilter extends OncePerRequestFilter {

    static final String MERCHANT_ID_ATTR = "paycore.merchantId";
    private final JdbcClient jdbc;
    private final String adminToken;
    private final String internalToken;

    public ReadModelAccessFilter(
            JdbcClient jdbc,
            @Value("${paycore.security.admin-token:}") String adminToken,
            @Value("${paycore.security.internal-token:}") String internalToken) {
        this.jdbc = jdbc;
        this.adminToken = safe(adminToken);
        this.internalToken = safe(internalToken);
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getRequestURI();
        return path == null || !(path.startsWith("/v1/"));
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String path = request.getRequestURI();
        if (path.startsWith("/v1/admin/")) {
            if (!authorized(adminToken, request.getHeader("X-PayCore-Admin-Token"))) {
                reject(response, adminToken.isEmpty() ? 503 : 401);
                return;
            }
        } else if (path.startsWith("/v1/internal/")) {
            if (!authorized(internalToken, request.getHeader("X-PayCore-Internal-Token"))) {
                reject(response, internalToken.isEmpty() ? 503 : 401);
                return;
            }
        } else if (path.startsWith("/v1/payments") || path.startsWith("/v1/merchants/me/")) {
            String rawKey = request.getHeader("X-API-Key");
            if (rawKey == null || rawKey.isBlank()) {
                reject(response, 401);
                return;
            }
            String keyHash = sha256Hex(rawKey.trim());
            var merchantId = jdbc.sql("SELECT id FROM merchants WHERE api_key_hash = :hash")
                    .param("hash", keyHash)
                    .query((rs, rowNum) -> rs.getObject("id", java.util.UUID.class))
                    .optional();
            if (merchantId.isEmpty()) {
                reject(response, 401);
                return;
            }
            request.setAttribute(MERCHANT_ID_ATTR, merchantId.get());
        }
        chain.doFilter(request, response);
    }

    private static boolean authorized(String expected, String actual) {
        return ServiceTokenValidator.matches(expected, actual);
    }

    private static String sha256Hex(String value) {
        try {
            return java.util.HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static String safe(String value) { return value == null ? "" : value.trim(); }

    private static void reject(HttpServletResponse response, int status) throws IOException {
        response.setStatus(status);
    }
}
