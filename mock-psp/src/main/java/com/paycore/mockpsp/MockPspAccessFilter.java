package com.paycore.mockpsp;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/** Limits PSP operations to trusted internal callers and fault controls to operators. */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 5)
public class MockPspAccessFilter extends OncePerRequestFilter {

    private final String internalToken;
    private final String adminToken;

    public MockPspAccessFilter(
            @Value("${paycore.security.internal-token:}") String internalToken,
            @Value("${paycore.security.admin-token:}") String adminToken) {
        this.internalToken = safe(internalToken);
        this.adminToken = safe(adminToken);
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getRequestURI();
        return path == null || !path.startsWith("/psp/v1/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        boolean adminRoute = request.getRequestURI().startsWith("/psp/v1/admin/");
        String configured = adminRoute ? adminToken : internalToken;
        String header = adminRoute ? "X-PayCore-Admin-Token" : "X-PayCore-Internal-Token";
        if (!matches(configured, request.getHeader(header))) {
            response.setStatus(configured.isEmpty() ? 503 : 401);
            return;
        }
        chain.doFilter(request, response);
    }

    private static boolean matches(String expected, String actual) {
        return !expected.isEmpty() && actual != null && MessageDigest.isEqual(
                expected.getBytes(StandardCharsets.UTF_8), actual.trim().getBytes(StandardCharsets.UTF_8));
    }

    private static String safe(String value) { return value == null ? "" : value.trim(); }
}
