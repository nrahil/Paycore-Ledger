package com.paycore.common.security;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/** Constant-time comparison for configured service and operator bearer tokens. */
public final class ServiceTokenValidator {
    private ServiceTokenValidator() {}

    public static boolean matches(String configuredToken, String suppliedToken) {
        if (configuredToken == null || configuredToken.isBlank() || suppliedToken == null) {
            return false;
        }
        byte[] expected = configuredToken.trim().getBytes(StandardCharsets.UTF_8);
        byte[] supplied = suppliedToken.trim().getBytes(StandardCharsets.UTF_8);
        return expected.length > 0 && MessageDigest.isEqual(expected, supplied);
    }
}
