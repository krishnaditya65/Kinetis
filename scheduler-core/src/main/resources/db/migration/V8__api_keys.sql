-- V8: API key authentication.
-- Keys are stored as SHA-256 hashes so the plaintext never reaches the DB.
-- A newly-issued key is shown once to the caller and then only the hash is kept.

CREATE TABLE api_keys (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    key_hash    TEXT        NOT NULL UNIQUE,   -- SHA-256(plain_key) in hex
    description TEXT,                          -- human-readable label
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ,                   -- NULL = never expires
    revoked     BOOLEAN     NOT NULL DEFAULT false
);

-- Fast lookup on every request (hot path)
CREATE INDEX idx_api_keys_hash ON api_keys (key_hash) WHERE NOT revoked;
