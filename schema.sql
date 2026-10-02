-- Explicit operator-applied migration; sync.py never migrates on startup.
CREATE TABLE IF NOT EXISTS allstate_ef_stream (
  stream TEXT PRIMARY KEY,
  cursor_id BIGINT NOT NULL CHECK (cursor_id >= 0),
  not_before TIMESTAMPTZ NOT NULL,
  config_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS allstate_ef_outbox (
  stream TEXT NOT NULL REFERENCES allstate_ef_stream(stream),
  billing_id BIGINT NOT NULL,
  event_key TEXT,
  source_hash TEXT NOT NULL,
  record_json TEXT NOT NULL,
  payload_json TEXT,
  state TEXT NOT NULL CHECK(state IN ('ready','held','sending','ambiguous','received_unverified','confirmed','conflict')),
  reason TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  attempted_at TIMESTAMPTZ,
  conversion_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY(stream,billing_id),
  UNIQUE(stream,event_key)
);
CREATE INDEX IF NOT EXISTS allstate_ef_outbox_pending ON allstate_ef_outbox(stream,state,billing_id);
