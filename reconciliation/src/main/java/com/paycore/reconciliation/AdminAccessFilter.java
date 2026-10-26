package com.paycore.reconciliation;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import com.paycore.common.security.ServiceTokenValidator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/** Keeps settlement import and discrepancy data behind operator authentication. */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 5)
public class AdminAccessFilter extends OncePerRequestFilter {

    private final String adminToken;

    public AdminAccessFilter(@Value("${paycore.security.admin-token:}") String adminToken) {
        this.adminToken = adminToken == null ? "" : adminToken.trim();
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getRequestURI();
        return path == null || !path.startsWith("/v1/admin/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String supplied = request.getHeader("X-PayCore-Admin-Token");
        boolean valid = ServiceTokenValidator.matches(adminToken, supplied);
        if (!valid) {
            response.setStatus(adminToken.isEmpty() ? 503 : 401);
            return;
        }
        chain.doFilter(request, response);
    }
}
