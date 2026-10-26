package com.paycore.payments.merchant;

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

/** Protects operational endpoints independently from merchant API credentials. */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 5)
public class AdminAuthFilter extends OncePerRequestFilter {

    private static final String ADMIN_PREFIX = "/v1/admin/";
    private static final String ADMIN_TOKEN_HEADER = "X-PayCore-Admin-Token";

    private final String configuredToken;

    public AdminAuthFilter(@Value("${paycore.security.admin-token:}") String configuredToken) {
        this.configuredToken = configuredToken == null ? "" : configuredToken.trim();
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getRequestURI();
        return path == null || !path.startsWith(ADMIN_PREFIX);
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain) throws ServletException, IOException {
        if (configuredToken.isEmpty()) {
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            return;
        }

        String suppliedToken = request.getHeader(ADMIN_TOKEN_HEADER);
        if (!ServiceTokenValidator.matches(configuredToken, suppliedToken)) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            return;
        }
        filterChain.doFilter(request, response);
    }
}
