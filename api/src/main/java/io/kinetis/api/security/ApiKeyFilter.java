package io.kinetis.api.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Servlet filter that enforces API key authentication on the {@code /jobs} and
 * {@code /workflows} endpoints. Pass the key in the {@code X-Api-Key} header.
 *
 * <p>Authentication is disabled when {@code scheduler.auth.enabled=false} (default),
 * so existing single-node and test deployments work without any key configuration.
 * Enable it in production via environment: {@code SCHED_AUTH_ENABLED=true}.
 */
@Component
public class ApiKeyFilter extends OncePerRequestFilter {

    private final ApiKeyStore keyStore;
    private final boolean authEnabled;

    public ApiKeyFilter(ApiKeyStore keyStore,
                        @Value("${scheduler.auth.enabled:false}") boolean authEnabled) {
        this.keyStore    = keyStore;
        this.authEnabled = authEnabled;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                     HttpServletResponse response,
                                     FilterChain chain) throws ServletException, IOException {
        if (!authEnabled) {
            chain.doFilter(request, response);
            return;
        }

        String path = request.getRequestURI();
        // Only protect the API endpoints; let actuator and UI through
        if (!path.startsWith("/jobs") && !path.startsWith("/workflows")) {
            chain.doFilter(request, response);
            return;
        }

        String key = request.getHeader("X-Api-Key");
        if (key == null || key.isBlank() || !keyStore.isValid(key)) {
            response.setStatus(HttpStatus.UNAUTHORIZED.value());
            response.setContentType("application/json");
            response.getWriter().write(
                    "{\"status\":401,\"title\":\"Unauthorized\",\"detail\":\"valid X-Api-Key required\"}");
            return;
        }

        chain.doFilter(request, response);
    }
}
