CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- -----------------------------------------------
-- Core tables (append-only, no UPDATE/DELETE)
-- -----------------------------------------------

CREATE TABLE rwa_assets (
  id UUID PRIMARY KEY,
  type VARCHAR(20) NOT NULL,
  name VARCHAR(256) NOT NULL,
  total_value DECIMAL(38,18) NOT NULL,
  jurisdiction VARCHAR(64) NOT NULL,
  custodian VARCHAR(256) NOT NULL,
  idempotency_key VARCHAR(256) UNIQUE,
  created_at TIMESTAMP NOT NULL
);

CREATE TABLE legal_wrappers (
  id UUID PRIMARY KEY,
  asset_id UUID REFERENCES rwa_assets(id),
  structure_type VARCHAR(20) NOT NULL,
  jurisdiction VARCHAR(64) NOT NULL,
  token_supply DECIMAL(38,18) NOT NULL,
  price_per_token DECIMAL(38,18) NOT NULL,
  status VARCHAR(20) NOT NULL,
  created_at TIMESTAMP NOT NULL
);

CREATE TABLE investor_compliance (
  id UUID PRIMARY KEY,
  investor_id UUID NOT NULL,
  wallet_address VARCHAR(256) NOT NULL,
  tier VARCHAR(20) NOT NULL,
  jurisdiction VARCHAR(64) NOT NULL,
  kyc_reference VARCHAR(256),
  idempotency_key VARCHAR(256) UNIQUE,
  created_at TIMESTAMP NOT NULL,
  expires_at TIMESTAMP NOT NULL
);

CREATE TABLE whitelisted_wallets (
  id UUID PRIMARY KEY,
  investor_id UUID NOT NULL,
  wallet_address VARCHAR(256) UNIQUE NOT NULL,
  tier VARCHAR(20) NOT NULL,
  created_at TIMESTAMP NOT NULL
);

CREATE TABLE token_mints (
  id UUID PRIMARY KEY,
  asset_id UUID REFERENCES rwa_assets(id),
  investor_id UUID NOT NULL,
  wallet_address VARCHAR(256) NOT NULL,
  token_amount DECIMAL(38,18) NOT NULL,
  fiat_received DECIMAL(38,18) NOT NULL,
  idempotency_key VARCHAR(256) UNIQUE,
  created_at TIMESTAMP NOT NULL
);

CREATE TABLE nav_calculations (
  id UUID PRIMARY KEY,
  asset_id UUID REFERENCES rwa_assets(id),
  total_value DECIMAL(38,18) NOT NULL,
  daily_yield DECIMAL(38,18) NOT NULL,
  yield_rate DECIMAL(38,18) NOT NULL,
  calculated_at TIMESTAMP NOT NULL,
  idempotency_key VARCHAR(256) UNIQUE
);

CREATE TABLE token_redemptions (
  id UUID PRIMARY KEY,
  asset_id UUID REFERENCES rwa_assets(id),
  investor_id UUID NOT NULL,
  wallet_address VARCHAR(256) NOT NULL,
  token_amount DECIMAL(38,18) NOT NULL,
  bank_account VARCHAR(256) NOT NULL,
  idempotency_key VARCHAR(256) UNIQUE,
  created_at TIMESTAMP NOT NULL
);

CREATE TABLE outbox_events (
  id UUID PRIMARY KEY,
  aggregate_id VARCHAR(256) NOT NULL,
  event_type VARCHAR(256) NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMP NOT NULL
);

-- -----------------------------------------------
-- State transitions (replaces all UPDATEs on status)
-- -----------------------------------------------

CREATE TABLE state_transitions (
  id UUID PRIMARY KEY,
  entity_type VARCHAR(20) NOT NULL,
  entity_id UUID NOT NULL,
  from_state VARCHAR(20),
  to_state VARCHAR(20) NOT NULL,
  metadata JSONB,
  request_id UUID,
  trace_id UUID,
  actor VARCHAR(256),
  created_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_state_transitions_entity
  ON state_transitions (entity_type, entity_id, created_at DESC);

-- -----------------------------------------------
-- Double-entry ledger (append-only financial journal)
-- -----------------------------------------------

CREATE TABLE ledger_entries (
  id UUID PRIMARY KEY,
  asset_id UUID REFERENCES rwa_assets(id),
  entry_type VARCHAR(20) NOT NULL,
  debit_account VARCHAR(256) NOT NULL,
  credit_account VARCHAR(256) NOT NULL,
  amount DECIMAL(38,18) NOT NULL,
  reference_id UUID NOT NULL,
  created_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_ledger_entries_asset
  ON ledger_entries (asset_id, entry_type);

CREATE INDEX idx_ledger_entries_debit
  ON ledger_entries (debit_account);

CREATE INDEX idx_ledger_entries_credit
  ON ledger_entries (credit_account);

-- -----------------------------------------------
-- Whitelist revocations (replaces DELETE on whitelisted_wallets)
-- -----------------------------------------------

CREATE TABLE whitelist_revocations (
  id UUID PRIMARY KEY,
  wallet_id UUID NOT NULL,
  investor_id UUID NOT NULL,
  reason VARCHAR(256) NOT NULL,
  created_at TIMESTAMP NOT NULL
);

-- -----------------------------------------------
-- Outbox published tracker (replaces UPDATE on outbox_events)
-- -----------------------------------------------

CREATE TABLE outbox_published (
  id UUID PRIMARY KEY,
  event_id UUID NOT NULL UNIQUE REFERENCES outbox_events(id),
  published_at TIMESTAMP NOT NULL
);

-- -----------------------------------------------
-- Prevent mutation trigger
-- -----------------------------------------------

CREATE OR REPLACE FUNCTION prevent_mutation()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION
    'Table % is append-only. UPDATE and DELETE are prohibited.',
    TG_TABLE_NAME;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_rwa_assets_immutable
  BEFORE UPDATE OR DELETE ON rwa_assets
  FOR EACH ROW EXECUTE FUNCTION prevent_mutation();

CREATE TRIGGER trg_legal_wrappers_immutable
  BEFORE UPDATE OR DELETE ON legal_wrappers
  FOR EACH ROW EXECUTE FUNCTION prevent_mutation();

CREATE TRIGGER trg_investor_compliance_immutable
  BEFORE UPDATE OR DELETE ON investor_compliance
  FOR EACH ROW EXECUTE FUNCTION prevent_mutation();

CREATE TRIGGER trg_whitelisted_wallets_immutable
  BEFORE UPDATE OR DELETE ON whitelisted_wallets
  FOR EACH ROW EXECUTE FUNCTION prevent_mutation();

CREATE TRIGGER trg_token_mints_immutable
  BEFORE UPDATE OR DELETE ON token_mints
  FOR EACH ROW EXECUTE FUNCTION prevent_mutation();

CREATE TRIGGER trg_nav_calculations_immutable
  BEFORE UPDATE OR DELETE ON nav_calculations
  FOR EACH ROW EXECUTE FUNCTION prevent_mutation();

CREATE TRIGGER trg_token_redemptions_immutable
  BEFORE UPDATE OR DELETE ON token_redemptions
  FOR EACH ROW EXECUTE FUNCTION prevent_mutation();

CREATE TRIGGER trg_outbox_events_immutable
  BEFORE UPDATE OR DELETE ON outbox_events
  FOR EACH ROW EXECUTE FUNCTION prevent_mutation();

CREATE TRIGGER trg_state_transitions_immutable
  BEFORE UPDATE OR DELETE ON state_transitions
  FOR EACH ROW EXECUTE FUNCTION prevent_mutation();

CREATE TRIGGER trg_ledger_entries_immutable
  BEFORE UPDATE OR DELETE ON ledger_entries
  FOR EACH ROW EXECUTE FUNCTION prevent_mutation();

CREATE TRIGGER trg_whitelist_revocations_immutable
  BEFORE UPDATE OR DELETE ON whitelist_revocations
  FOR EACH ROW EXECUTE FUNCTION prevent_mutation();

CREATE TRIGGER trg_outbox_published_immutable
  BEFORE UPDATE OR DELETE ON outbox_published
  FOR EACH ROW EXECUTE FUNCTION prevent_mutation();

-- -----------------------------------------------
-- DLQ tables (append-only)
-- -----------------------------------------------

CREATE TABLE outbox_publish_attempts (
  id UUID PRIMARY KEY,
  event_id UUID NOT NULL REFERENCES outbox_events(id),
  error_message TEXT NOT NULL,
  attempted_at TIMESTAMP NOT NULL
);

CREATE TABLE outbox_dlq (
  id UUID PRIMARY KEY,
  event_id UUID NOT NULL UNIQUE REFERENCES outbox_events(id),
  error_message TEXT NOT NULL,
  attempts INTEGER NOT NULL,
  created_at TIMESTAMP NOT NULL
);

CREATE TRIGGER trg_outbox_publish_attempts_immutable
  BEFORE UPDATE OR DELETE ON outbox_publish_attempts
  FOR EACH ROW EXECUTE FUNCTION prevent_mutation();

CREATE TRIGGER trg_outbox_dlq_immutable
  BEFORE UPDATE OR DELETE ON outbox_dlq
  FOR EACH ROW EXECUTE FUNCTION prevent_mutation();

-- -----------------------------------------------
-- Views — derived state
-- -----------------------------------------------

-- Current state per entity (latest state_transition row)
CREATE VIEW v_current_state AS
SELECT DISTINCT ON (entity_type, entity_id)
  entity_type,
  entity_id,
  to_state AS current_state,
  metadata,
  created_at
FROM state_transitions
ORDER BY entity_type, entity_id, created_at DESC;

-- Investor balance per asset from ledger entries
-- Debits to investor account increase balance;
-- credits from investor account decrease balance.
CREATE VIEW v_investor_balance AS
SELECT
  asset_id,
  account_id AS investor_id,
  SUM(balance_change) AS balance
FROM (
  SELECT asset_id, debit_account AS account_id, amount AS balance_change
  FROM ledger_entries
  WHERE debit_account LIKE 'investor:%'
  UNION ALL
  SELECT asset_id, credit_account AS account_id, -amount AS balance_change
  FROM ledger_entries
  WHERE credit_account LIKE 'investor:%'
) sub
GROUP BY asset_id, account_id;

-- Reserved (pending) supply per asset from token_mints
-- that have not failed
CREATE VIEW v_minted_supply AS
SELECT
  tm.asset_id,
  COALESCE(SUM(tm.token_amount), 0) AS reserved_supply
FROM token_mints tm
WHERE NOT EXISTS (
  SELECT 1 FROM v_current_state cs
  WHERE cs.entity_type = 'mint'
    AND cs.entity_id = tm.id
    AND cs.current_state = 'failed'
)
GROUP BY tm.asset_id;

-- Active whitelists (not revoked)
CREATE VIEW v_active_whitelists AS
SELECT ww.*
FROM whitelisted_wallets ww
LEFT JOIN whitelist_revocations wr
  ON wr.wallet_id = ww.id
WHERE wr.id IS NULL;

-- Unpublished outbox events (excludes dead-lettered)
CREATE VIEW v_unpublished_events AS
SELECT oe.*
FROM outbox_events oe
LEFT JOIN outbox_published op
  ON op.event_id = oe.id
LEFT JOIN outbox_dlq dlq
  ON dlq.event_id = oe.id
WHERE op.id IS NULL
AND dlq.id IS NULL;

-- Asset current value: initial total_value + accumulated yields
CREATE VIEW v_asset_current_value AS
SELECT
  a.id AS asset_id,
  a.total_value + COALESCE(nav.total_yield, 0) AS current_value
FROM rwa_assets a
LEFT JOIN (
  SELECT asset_id, SUM(daily_yield) AS total_yield
  FROM nav_calculations
  GROUP BY asset_id
) nav ON nav.asset_id = a.id;
