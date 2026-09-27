CREATE SCHEMA IF NOT EXISTS issue3_lab;

CREATE TABLE IF NOT EXISTS issue3_lab.capacity_resources (
  resource_id text PRIMARY KEY,
  capacity integer NOT NULL CHECK (capacity > 0)
);

CREATE TABLE IF NOT EXISTS issue3_lab.synthetic_wallets (
  wallet_id text PRIMARY KEY,
  available_amount bigint NOT NULL CHECK (available_amount >= 0),
  reserved_amount bigint NOT NULL DEFAULT 0 CHECK (reserved_amount >= 0),
  consumed_amount bigint NOT NULL DEFAULT 0 CHECK (consumed_amount >= 0)
);

CREATE TABLE IF NOT EXISTS issue3_lab.operations (
  operation_id text PRIMARY KEY,
  resource_id text NOT NULL REFERENCES issue3_lab.capacity_resources(resource_id),
  actor_id text NOT NULL,
  wallet_id text NOT NULL REFERENCES issue3_lab.synthetic_wallets(wallet_id),
  amount bigint NOT NULL CHECK (amount > 0),
  payload jsonb NOT NULL,
  state text NOT NULL CHECK (state IN ('PENDING', 'PROCESSING', 'CONFIRMED', 'REJECTED', 'EXPIRED')),
  reason text CHECK (
    reason IS NULL OR reason IN ('CAPACITY_UNAVAILABLE', 'INSUFFICIENT_FUNDS', 'HOLD_EXPIRED')
  ),
  hold_expires_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS operations_resource_state_idx
  ON issue3_lab.operations(resource_id, state, hold_expires_at);

CREATE TABLE IF NOT EXISTS issue3_lab.synthetic_ledger (
  ledger_entry_id bigserial PRIMARY KEY,
  operation_id text NOT NULL REFERENCES issue3_lab.operations(operation_id),
  wallet_id text NOT NULL REFERENCES issue3_lab.synthetic_wallets(wallet_id),
  event_type text NOT NULL CHECK (event_type IN ('RESERVED', 'CONSUMED', 'RELEASED')),
  amount bigint NOT NULL CHECK (amount > 0),
  created_at timestamptz NOT NULL,
  UNIQUE (operation_id, event_type)
);

CREATE OR REPLACE FUNCTION issue3_lab.prevent_synthetic_ledger_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION 'synthetic ledger is append-only';
  RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS synthetic_ledger_append_only
  ON issue3_lab.synthetic_ledger;

CREATE TRIGGER synthetic_ledger_append_only
  BEFORE UPDATE OR DELETE ON issue3_lab.synthetic_ledger
  FOR EACH ROW
  EXECUTE FUNCTION issue3_lab.prevent_synthetic_ledger_mutation();
