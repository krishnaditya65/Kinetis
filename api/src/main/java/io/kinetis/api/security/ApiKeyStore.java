package io.kinetis.api.security;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HexFormat;

/**
 * Issues and validates API keys. Keys are URL-safe Base64 random tokens;
 * only their SHA-256 hash is stored in the DB.
 */
@Component
public class ApiKeyStore {

    private final JdbcTemplate jdbc;

    public ApiKeyStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Issue a new API key. Returns the plaintext key (shown once to the caller).
     * Only the hash is persisted.
     */
    public String issue(String description) {
        byte[] raw = new byte[32];
        new SecureRandom().nextBytes(raw);
        String plainKey = "knt_" + Base64.getUrlEncoder().withoutPadding().encodeToString(raw);
        jdbc.update("""
                INSERT INTO api_keys (key_hash, description) VALUES (?, ?)
                """, hash(plainKey), description);
        return plainKey;
    }

    /**
     * @return true if the key exists, is not revoked, and has not expired
     */
    public boolean isValid(String plainKey) {
        if (plainKey == null || plainKey.isBlank()) return false;
        Integer count = jdbc.queryForObject("""
                SELECT count(*) FROM api_keys
                WHERE key_hash = ?
                  AND NOT revoked
                  AND (expires_at IS NULL OR expires_at > now())
                """, Integer.class, hash(plainKey));
        return count != null && count > 0;
    }

    public void revoke(String plainKey) {
        jdbc.update("UPDATE api_keys SET revoked = true WHERE key_hash = ?", hash(plainKey));
    }

    static String hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(md.digest(key.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
