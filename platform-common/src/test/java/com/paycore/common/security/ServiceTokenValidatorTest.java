package com.paycore.common.security;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ServiceTokenValidatorTest {
    @Test
    void comparesTrimmedTokensAndRejectsMissingConfigurationOrCredentials() {
        assertThat(ServiceTokenValidator.matches(" operator-secret ", "operator-secret ")).isTrue();
        assertThat(ServiceTokenValidator.matches("", "operator-secret")).isFalse();
        assertThat(ServiceTokenValidator.matches("operator-secret", null)).isFalse();
        assertThat(ServiceTokenValidator.matches(null, "operator-secret")).isFalse();
        assertThat(ServiceTokenValidator.matches("operator-secret", "different-secret")).isFalse();
    }
}
