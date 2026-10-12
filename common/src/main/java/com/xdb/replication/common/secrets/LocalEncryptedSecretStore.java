package com.xdb.replication.common.secrets;

import org.springframework.jdbc.core.JdbcTemplate;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * Development secret store: AES-256-GCM encrypted values in table {@code xdb_secret}.
 * The key comes from {@code xdb.secrets.local-key} (an env var / Kubernetes Secret),
 * never from the database itself.
 */
public class LocalEncryptedSecretStore implements SecretStore {

    private static final int IV_BYTES = 12;
    private static final int TAG_BITS = 128;

    private final JdbcTemplate jdbc;
    private final SecretKeySpec key;
    private final SecureRandom random = new SecureRandom();

    public LocalEncryptedSecretStore(JdbcTemplate jdbc, String passphrase) {
        if (passphrase == null || passphrase.length() < 16) {
            throw new IllegalArgumentException("xdb.secrets.local-key must be at least 16 characters");
        }
        this.jdbc = jdbc;
        this.key = new SecretKeySpec(sha256(passphrase), "AES");
    }

    @Override
    public String put(String ref, String secret) {
        String encrypted = encrypt(secret);
        int updated = jdbc.update("UPDATE xdb_secret SET ciphertext = ?, updated_at = now() WHERE ref = ?", encrypted, ref);
        if (updated == 0) {
            jdbc.update("INSERT INTO xdb_secret (ref, ciphertext) VALUES (?, ?)", ref, encrypted);
        }
        return ref;
    }

    @Override
    public String get(String ref) {
        String encrypted = jdbc.queryForObject("SELECT ciphertext FROM xdb_secret WHERE ref = ?", String.class, ref);
        return decrypt(encrypted);
    }

    @Override
    public void delete(String ref) {
        jdbc.update("DELETE FROM xdb_secret WHERE ref = ?", ref);
    }

    private String encrypt(String plain) {
        try {
            byte[] iv = new byte[IV_BYTES];
            random.nextBytes(iv);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, iv));
            byte[] ct = cipher.doFinal(plain.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(ByteBuffer.allocate(iv.length + ct.length).put(iv).put(ct).array());
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Secret encryption failed", e);
        }
    }

    private String decrypt(String encoded) {
        try {
            byte[] all = Base64.getDecoder().decode(encoded);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, all, 0, IV_BYTES));
            return new String(cipher.doFinal(all, IV_BYTES, all.length - IV_BYTES), StandardCharsets.UTF_8);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Secret decryption failed (wrong xdb.secrets.local-key?)", e);
        }
    }

    private static byte[] sha256(String s) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }
}
