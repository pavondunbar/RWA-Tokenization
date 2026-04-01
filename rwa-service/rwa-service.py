import os
import uuid
import json
import logging
from datetime import datetime, timezone
from enum import Enum
from decimal import Decimal
from types import SimpleNamespace

# -----------------------------------------------
# Enums — State Machines
# -----------------------------------------------

class AssetType(str, Enum):
    REAL_ESTATE    = "real_estate"
    TREASURY_BOND  = "treasury_bond"
    PRIVATE_EQUITY = "private_equity"
    COMMODITY      = "commodity"
    FUND           = "fund"

class AssetStatus(str, Enum):
    PENDING_LEGAL  = "pending_legal"
    PENDING_AUDIT  = "pending_audit"
    APPROVED       = "approved"
    TOKENIZED      = "tokenized"
    REDEEMED       = "redeemed"

class LegalStructure(str, Enum):
    SPV   = "spv"
    TRUST = "trust"
    LLC   = "llc"
    FUND  = "fund"

class InvestorTier(str, Enum):
    RETAIL        = "retail"
    ACCREDITED    = "accredited"
    QUALIFIED     = "qualified"
    INSTITUTIONAL = "institutional"

class ComplianceStatus(str, Enum):
    PENDING  = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED  = "expired"

class MintStatus(str, Enum):
    PENDING   = "pending"
    APPROVED  = "approved"
    SIGNED    = "signed"
    BROADCAST = "broadcast"
    CONFIRMED = "confirmed"
    FAILED    = "failed"

class RedemptionStatus(str, Enum):
    PENDING   = "pending"
    APPROVED  = "approved"
    BURNING   = "burning"
    BURNED    = "burned"
    SETTLED   = "settled"
    FAILED    = "failed"


# -----------------------------------------------
# Custom Exceptions
# -----------------------------------------------

class InvalidCustodianError(Exception):
    pass

class InvalidStateError(Exception):
    pass

class WalletNotWhitelistedError(Exception):
    pass

class InsufficientTokenSupplyError(Exception):
    pass

class SanctionedInvestorError(Exception):
    pass

class KYCFailedError(Exception):
    pass

class InsufficientTokenBalanceError(Exception):
    pass

class UnauthorizedError(Exception):
    pass


# -----------------------------------------------
# RBAC — Role-Based Access Control
# -----------------------------------------------

class Role(str, Enum):
    ADMIN      = "admin"
    SYSTEM     = "system"
    SIGNER     = "signer"
    COMPLIANCE = "compliance"


class Actor:
    def __init__(self, actor_id, role, name):
        self.actor_id = actor_id
        self.role = role
        self.name = name

    def __str__(self):
        return f"{self.role.value}:{self.name}"


def require_role(actor, *allowed_roles):
    if actor.role not in allowed_roles:
        raise UnauthorizedError(
            f"Role {actor.role.value} cannot perform "
            f"this action. Required: "
            f"{[r.value for r in allowed_roles]}"
        )


# -----------------------------------------------
# PostgresDB — Real Postgres Connection
# -----------------------------------------------

class PostgresDB:
    """Thin wrapper around psycopg2 providing the .query()
    and .transaction() interface all service classes expect."""

    def __init__(self, dsn):
        self.dsn = dsn

    def _connect(self):
        import psycopg2
        import psycopg2.extras
        psycopg2.extras.register_uuid()
        return psycopg2.connect(self.dsn)

    def query(self, sql, params=None):
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                if cur.description is None:
                    return None
                cols = [d[0] for d in cur.description]
                row = cur.fetchone()
                if row is None:
                    return None
                return SimpleNamespace(**dict(zip(cols, row)))
        finally:
            conn.close()

    def transaction(self):
        return PostgresTransaction(self._connect())


class PostgresTransaction:
    """Context manager wrapping a psycopg2 connection
    with commit/rollback semantics."""

    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.conn.commit()
        else:
            self.conn.rollback()
        self.conn.close()

    def query(self, sql, params=None):
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            if cur.description is None:
                return None
            cols = [d[0] for d in cur.description]
            row = cur.fetchone()
            if row is None:
                return None
            return SimpleNamespace(**dict(zip(cols, row)))

    def execute(self, sql, params=None):
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            if cur.description is None:
                return None
            cols = [d[0] for d in cur.description]
            row = cur.fetchone()
            if row is None:
                return None
            return SimpleNamespace(**dict(zip(cols, row)))


# -----------------------------------------------
# Append-Only Helpers
# -----------------------------------------------

def get_current_state(conn, entity_type, entity_id):
    """Returns the current state of an entity from
    the latest state_transition row, or None."""
    row = conn.query(
        "SELECT to_state, metadata "
        "FROM state_transitions "
        "WHERE entity_type = %s AND entity_id = %s "
        "ORDER BY created_at DESC "
        "LIMIT 1",
        (entity_type, entity_id)
    )
    if row is None:
        return None
    return row.to_state


def insert_state_transition(
    conn, entity_type, entity_id,
    from_state, to_state, metadata=None,
    request_id=None, trace_id=None, actor=None
):
    """Appends a state_transition row."""
    conn.execute(
        "INSERT INTO state_transitions "
        "(id, entity_type, entity_id, from_state, "
        " to_state, metadata, request_id, "
        " trace_id, actor, created_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (
            uuid.uuid4(),
            entity_type,
            entity_id,
            from_state,
            to_state,
            json.dumps(metadata) if metadata else None,
            request_id,
            trace_id,
            actor,
            datetime.now(timezone.utc),
        )
    )


def insert_ledger_entry(
    conn, asset_id, entry_type,
    debit_account, credit_account,
    amount, reference_id
):
    """Appends a double-entry ledger row."""
    conn.execute(
        "INSERT INTO ledger_entries "
        "(id, asset_id, entry_type, debit_account, "
        " credit_account, amount, reference_id, created_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
        (
            uuid.uuid4(),
            asset_id,
            entry_type,
            debit_account,
            credit_account,
            str(amount),
            reference_id,
            datetime.now(timezone.utc),
        )
    )


def get_investor_balance(conn, asset_id, investor_id):
    """Derives investor balance from ledger_entries.
    Debits to the investor account add tokens;
    credits from the investor account subtract tokens."""
    account = f"investor:{investor_id}"
    row = conn.query(
        "SELECT COALESCE(SUM(CASE "
        "  WHEN debit_account = %s THEN amount "
        "  WHEN credit_account = %s THEN -amount "
        "  ELSE 0 END), 0) AS balance "
        "FROM ledger_entries "
        "WHERE asset_id = %s "
        "AND (debit_account = %s OR credit_account = %s)",
        (account, account, asset_id, account, account)
    )
    return Decimal(row.balance)


def get_reserved_supply(conn, asset_id):
    """Derives reserved supply from token_mints whose
    state is not 'failed'."""
    row = conn.query(
        "SELECT COALESCE(SUM(tm.token_amount), 0) "
        "  AS reserved "
        "FROM token_mints tm "
        "WHERE tm.asset_id = %s "
        "AND NOT EXISTS ("
        "  SELECT 1 FROM state_transitions st "
        "  WHERE st.entity_type = 'mint' "
        "  AND st.entity_id = tm.id "
        "  AND st.to_state = 'failed' "
        "  AND st.created_at = ("
        "    SELECT MAX(st2.created_at) "
        "    FROM state_transitions st2 "
        "    WHERE st2.entity_type = 'mint' "
        "    AND st2.entity_id = tm.id"
        "  )"
        ")",
        (asset_id,)
    )
    return Decimal(row.reserved)


def get_pending_redemptions(conn, asset_id, investor_id):
    """Sum of token_amounts from redemptions that are
    not yet settled or failed."""
    row = conn.query(
        "SELECT COALESCE(SUM(tr.token_amount), 0) "
        "  AS pending "
        "FROM token_redemptions tr "
        "WHERE tr.asset_id = %s "
        "AND tr.investor_id = %s "
        "AND NOT EXISTS ("
        "  SELECT 1 FROM state_transitions st "
        "  WHERE st.entity_type = 'redemption' "
        "  AND st.entity_id = tr.id "
        "  AND st.to_state IN ('settled', 'failed') "
        "  AND st.created_at = ("
        "    SELECT MAX(st2.created_at) "
        "    FROM state_transitions st2 "
        "    WHERE st2.entity_type = 'redemption' "
        "    AND st2.entity_id = tr.id"
        "  )"
        ")",
        (asset_id, investor_id)
    )
    return Decimal(row.pending)


def get_asset_current_value(conn, asset_id):
    """Derives current asset value: initial total_value
    plus accumulated NAV yields."""
    row = conn.query(
        "SELECT a.total_value + COALESCE(nav.total_yield, 0)"
        "  AS current_value "
        "FROM rwa_assets a "
        "LEFT JOIN ("
        "  SELECT asset_id, SUM(daily_yield) AS total_yield "
        "  FROM nav_calculations "
        "  GROUP BY asset_id"
        ") nav ON nav.asset_id = a.id "
        "WHERE a.id = %s",
        (asset_id,)
    )
    return Decimal(row.current_value)


# -----------------------------------------------
# KafkaSigningQueue — Real Kafka Producer
# -----------------------------------------------

class KafkaSigningQueue:
    """Wraps KafkaProducer with the same send() signature
    that TokenMintingService and TokenRedemptionService expect."""

    def __init__(self, producer):
        self.producer = producer

    def send(self, message_body, message_group_id,
             message_deduplication_id):
        self.producer.send(
            topic="rwa.signing_requests",
            key=message_group_id.encode(),
            value=json.dumps(message_body).encode(),
            headers=[
                (
                    "deduplication_id",
                    message_deduplication_id.encode()
                ),
            ],
        )
        self.producer.flush()


# -----------------------------------------------
# RWA Registry — Register Real World Assets
# -----------------------------------------------

class RWARegistry:
    """
    Registry of real-world assets awaiting or undergoing tokenization.
    Each asset maps to one token contract on-chain.

    BlackRock BUIDL example:
      - Asset type: FUND
      - Name: BlackRock USD Institutional Digital Liquidity Fund
      - Total value: $2.9B+
      - Custodian: BNY Mellon
      - Jurisdiction: Delaware, United States
    """

    def __init__(self, db, custodian_registry):
        self.db = db
        self.custodian_registry = custodian_registry

    def register_asset(
        self, asset_type, name, total_value,
        jurisdiction, custodian, idempotency_key,
        actor=None, trace_id=None
    ):
        if actor:
            require_role(actor, Role.ADMIN)

        existing = self.db.query(
            "SELECT * FROM rwa_assets "
            "WHERE idempotency_key = %s",
            (idempotency_key,)
        )
        if existing:
            return existing

        if not self._validate_custodian(custodian):
            raise InvalidCustodianError(
                f"Custodian {custodian} is not approved"
            )

        request_id = uuid.uuid4()

        with self.db.transaction() as conn:

            asset_id = uuid.uuid4()
            asset = conn.execute(
                "INSERT INTO rwa_assets "
                "(id, type, name, total_value, jurisdiction, "
                " custodian, idempotency_key, created_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                "RETURNING *",
                (
                    asset_id,
                    asset_type.value,
                    name,
                    str(total_value),
                    jurisdiction,
                    custodian,
                    idempotency_key,
                    datetime.now(timezone.utc),
                )
            )

            actor_str = str(actor) if actor else None
            insert_state_transition(
                conn, 'asset', asset_id,
                None, AssetStatus.PENDING_LEGAL.value,
                request_id=request_id,
                trace_id=trace_id,
                actor=actor_str,
            )

            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, payload, "
                " created_at) "
                "VALUES (%s,%s,%s,%s,%s)",
                (
                    uuid.uuid4(),
                    str(asset_id),
                    "rwa.registered",
                    json.dumps({
                        "asset_id":     str(asset_id),
                        "type":         asset_type.value,
                        "name":         name,
                        "total_value":  str(total_value),
                        "jurisdiction": jurisdiction,
                        "custodian":    custodian,
                        "request_id":   str(request_id),
                        "trace_id":     str(trace_id)
                                        if trace_id else None,
                        "actor":        actor_str,
                    }),
                    datetime.now(timezone.utc),
                )
            )

        return asset

    def _validate_custodian(self, custodian):
        return bool(custodian)


# -----------------------------------------------
# Legal Wrapper Service — SPV / Trust / Fund
# -----------------------------------------------

class LegalWrapperService:
    """
    Creates the legal entity that OWNS the real-world asset.
    This is the bridge between off-chain legal ownership
    and on-chain token representation.

    Without this layer the token is just a receipt —
    no enforceable legal claim to the underlying asset.

    BlackRock BUIDL structure:
      - BlackRock creates a regulated fund
      - Fund holds US Treasury bills
      - Token = fund share with legal claim
      - Each token = $1 of fund NAV
      - Yield accrues by rebasing token balance daily
    """

    def __init__(self, db):
        self.db = db

    def create_legal_wrapper(
        self, asset_id, structure_type,
        jurisdiction, token_supply,
        actor=None, trace_id=None
    ):
        if actor:
            require_role(actor, Role.ADMIN)

        request_id = uuid.uuid4()

        with self.db.transaction() as conn:

            asset = conn.query(
                "SELECT id FROM rwa_assets "
                "WHERE id = %s "
                "FOR UPDATE",
                (asset_id,)
            )

            current = get_current_state(conn, 'asset', asset_id)
            if current != AssetStatus.PENDING_LEGAL.value:
                raise InvalidStateError(
                    f"Expected pending_legal, got {current}"
                )

            total_value = get_asset_current_value(conn, asset_id)
            price_per_token = (
                total_value /
                Decimal(str(token_supply))
            )

            wrapper_id = uuid.uuid4()
            conn.execute(
                "INSERT INTO legal_wrappers "
                "(id, asset_id, structure_type, jurisdiction, "
                " token_supply, price_per_token, "
                " status, created_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    wrapper_id,
                    asset_id,
                    structure_type.value,
                    jurisdiction,
                    str(token_supply),
                    str(price_per_token),
                    "active",
                    datetime.now(timezone.utc),
                )
            )

            actor_str = str(actor) if actor else None
            insert_state_transition(
                conn, 'asset', asset_id,
                AssetStatus.PENDING_LEGAL.value,
                AssetStatus.PENDING_AUDIT.value,
                request_id=request_id,
                trace_id=trace_id,
                actor=actor_str,
            )

            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, payload, "
                " created_at) "
                "VALUES (%s,%s,%s,%s,%s)",
                (
                    uuid.uuid4(),
                    str(asset_id),
                    "rwa.legal_wrapper_created",
                    json.dumps({
                        "asset_id":        str(asset_id),
                        "wrapper_id":      str(wrapper_id),
                        "structure":       structure_type.value,
                        "token_supply":    str(token_supply),
                        "price_per_token": str(price_per_token),
                        "request_id":      str(request_id),
                        "trace_id":        str(trace_id)
                                           if trace_id
                                           else None,
                        "actor":           actor_str,
                    }),
                    datetime.now(timezone.utc),
                )
            )

        return wrapper_id


# -----------------------------------------------
# KYC Compliance Service — Investor Onboarding
# -----------------------------------------------

class KYCComplianceService:
    """
    RWA tokens are NOT permissionless like DeFi.
    Every investor must pass KYC/AML before holding tokens.
    Both sender AND receiver must be whitelisted on every
    transfer. This is enforced at the smart contract level.

    BlackRock BUIDL requirements:
      - Institutional investors only
      - Minimum $5M investment
      - Full KYC/AML via Securitize platform
      - Whitelisted Ethereum wallet addresses only
      - Transfers only between whitelisted addresses
      - KYC expires annually — must re-verify
    """

    def __init__(self, db, kyc_provider, sanctions_checker):
        self.db = db
        self.kyc_provider = kyc_provider
        self.sanctions_checker = sanctions_checker

    def onboard_investor(
        self, investor_id, wallet_address,
        jurisdiction, investor_tier,
        idempotency_key,
        actor=None, trace_id=None
    ):
        if actor:
            require_role(actor, Role.COMPLIANCE)

        existing = self.db.query(
            "SELECT * FROM investor_compliance "
            "WHERE idempotency_key = %s",
            (idempotency_key,)
        )
        if existing:
            return existing

        sanctions_result = self.sanctions_checker.screen(
            investor_id, jurisdiction
        )
        if sanctions_result.is_sanctioned:
            raise SanctionedInvestorError(
                f"Investor {investor_id} is on a sanctions list"
            )

        kyc_result = self.kyc_provider.verify(
            investor_id, investor_tier
        )
        if not kyc_result.passed:
            raise KYCFailedError(kyc_result.reason)

        request_id = uuid.uuid4()

        with self.db.transaction() as conn:

            compliance_id = uuid.uuid4()
            conn.execute(
                "INSERT INTO investor_compliance "
                "(id, investor_id, wallet_address, tier, "
                " jurisdiction, kyc_reference, "
                " idempotency_key, created_at, expires_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    compliance_id,
                    investor_id,
                    wallet_address,
                    investor_tier.value,
                    jurisdiction,
                    kyc_result.reference_id,
                    idempotency_key,
                    datetime.now(timezone.utc),
                    kyc_result.expiry_date,
                )
            )

            actor_str = str(actor) if actor else None
            insert_state_transition(
                conn, 'compliance', compliance_id,
                None, ComplianceStatus.APPROVED.value,
                request_id=request_id,
                trace_id=trace_id,
                actor=actor_str,
            )

            conn.execute(
                "INSERT INTO whitelisted_wallets "
                "(id, investor_id, wallet_address, "
                " tier, created_at) "
                "VALUES (%s,%s,%s,%s,%s)",
                (
                    uuid.uuid4(),
                    investor_id,
                    wallet_address,
                    investor_tier.value,
                    datetime.now(timezone.utc),
                )
            )

            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, payload, "
                " created_at) "
                "VALUES (%s,%s,%s,%s,%s)",
                (
                    uuid.uuid4(),
                    str(compliance_id),
                    "investor.whitelisted",
                    json.dumps({
                        "investor_id":    str(investor_id),
                        "wallet_address": wallet_address,
                        "tier":           investor_tier.value,
                        "jurisdiction":   jurisdiction,
                        "request_id":     str(request_id),
                        "trace_id":       str(trace_id)
                                          if trace_id
                                          else None,
                        "actor":          actor_str,
                    }),
                    datetime.now(timezone.utc),
                )
            )

        return compliance_id

    def can_transfer(self, from_wallet, to_wallet):
        """Both sender AND receiver must be actively whitelisted
        (not revoked) for a transfer to proceed."""
        sender_approved = self.db.query(
            "SELECT ww.id FROM whitelisted_wallets ww "
            "LEFT JOIN whitelist_revocations wr "
            "  ON wr.wallet_id = ww.id "
            "WHERE ww.wallet_address = %s "
            "AND wr.id IS NULL",
            (from_wallet,)
        )
        receiver_approved = self.db.query(
            "SELECT ww.id FROM whitelisted_wallets ww "
            "LEFT JOIN whitelist_revocations wr "
            "  ON wr.wallet_id = ww.id "
            "WHERE ww.wallet_address = %s "
            "AND wr.id IS NULL",
            (to_wallet,)
        )
        return bool(sender_approved) and bool(receiver_approved)

    def revoke_whitelist(
        self, investor_id, reason,
        actor=None, trace_id=None
    ):
        """Revokes investor whitelist status by inserting
        revocation records instead of deleting rows."""
        if actor:
            require_role(actor, Role.COMPLIANCE)

        request_id = uuid.uuid4()

        with self.db.transaction() as conn:

            wallet = conn.query(
                "SELECT ww.id FROM whitelisted_wallets ww "
                "LEFT JOIN whitelist_revocations wr "
                "  ON wr.wallet_id = ww.id "
                "WHERE ww.investor_id = %s "
                "AND wr.id IS NULL",
                (investor_id,)
            )

            if wallet:
                conn.execute(
                    "INSERT INTO whitelist_revocations "
                    "(id, wallet_id, investor_id, reason, "
                    " created_at) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (
                        uuid.uuid4(),
                        wallet.id,
                        investor_id,
                        reason,
                        datetime.now(timezone.utc),
                    )
                )

            actor_str = str(actor) if actor else None
            compliance = conn.query(
                "SELECT id FROM investor_compliance "
                "WHERE investor_id = %s "
                "ORDER BY created_at DESC LIMIT 1",
                (investor_id,)
            )
            if compliance:
                insert_state_transition(
                    conn, 'compliance', compliance.id,
                    ComplianceStatus.APPROVED.value,
                    ComplianceStatus.EXPIRED.value,
                    {"reason": reason},
                    request_id=request_id,
                    trace_id=trace_id,
                    actor=actor_str,
                )

            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, payload, "
                " created_at) "
                "VALUES (%s,%s,%s,%s,%s)",
                (
                    uuid.uuid4(),
                    str(investor_id),
                    "investor.whitelist_revoked",
                    json.dumps({
                        "investor_id": str(investor_id),
                        "reason":      reason,
                        "request_id":  str(request_id),
                        "trace_id":    str(trace_id)
                                       if trace_id
                                       else None,
                        "actor":       actor_str,
                    }),
                    datetime.now(timezone.utc),
                )
            )


# -----------------------------------------------
# Token Minting Service — Issue Tokens to Investors
# -----------------------------------------------

class TokenMintingService:
    """
    Mints RWA tokens to a whitelisted investor wallet.
    Triggered when investor wires fiat to the fund.

    Flow mirrors crypto withdrawal exactly:
      - Idempotency check first
      - Lock asset row FOR UPDATE
      - Derive reserved supply from token_mints
      - Create mint record + outbox atomically
      - Publish to signing queue outside transaction
      - Signing service calls ERC-1400 mint() on-chain

    BlackRock BUIDL example:
      - Investor wires $10M USD to fund
      - Fund receives fiat, buys T-bills with proceeds
      - Mints 10,000,000 BUIDL tokens to investor wallet
      - Each token = $1 NAV, rebases daily with T-bill yield
    """

    def __init__(self, db, compliance_service, signing_queue):
        self.db = db
        self.compliance = compliance_service
        self.signing_queue = signing_queue

    def mint_tokens(
        self, asset_id, investor_id, wallet_address,
        token_amount, fiat_received, idempotency_key,
        actor=None, trace_id=None
    ):
        if actor:
            require_role(actor, Role.ADMIN)

        existing = self.db.query(
            "SELECT * FROM token_mints "
            "WHERE idempotency_key = %s",
            (idempotency_key,)
        )
        if existing:
            return existing

        if not self.compliance.can_transfer(
            "ISSUER_WALLET", wallet_address
        ):
            raise WalletNotWhitelistedError(
                f"Wallet {wallet_address} is not whitelisted"
            )

        request_id = uuid.uuid4()

        with self.db.transaction() as conn:

            conn.query(
                "SELECT id FROM rwa_assets "
                "WHERE id = %s "
                "FOR UPDATE",
                (asset_id,)
            )

            total_supply_row = conn.query(
                "SELECT token_supply "
                "FROM legal_wrappers "
                "WHERE asset_id = %s",
                (asset_id,)
            )
            total_supply = Decimal(total_supply_row.token_supply)

            reserved = get_reserved_supply(conn, asset_id)

            available = total_supply - reserved
            if available < Decimal(str(token_amount)):
                raise InsufficientTokenSupplyError(
                    f"Only {available} tokens remaining"
                )

            mint_id = uuid.uuid4()
            mint = conn.execute(
                "INSERT INTO token_mints "
                "(id, asset_id, investor_id, wallet_address, "
                " token_amount, fiat_received, "
                " idempotency_key, created_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                "RETURNING *",
                (
                    mint_id,
                    asset_id,
                    investor_id,
                    wallet_address,
                    str(token_amount),
                    str(fiat_received),
                    idempotency_key,
                    datetime.now(timezone.utc),
                )
            )

            actor_str = str(actor) if actor else None
            insert_state_transition(
                conn, 'mint', mint_id,
                None, MintStatus.PENDING.value,
                request_id=request_id,
                trace_id=trace_id,
                actor=actor_str,
            )

            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, payload, "
                " created_at) "
                "VALUES (%s,%s,%s,%s,%s)",
                (
                    uuid.uuid4(),
                    str(mint_id),
                    "token.mint_requested",
                    json.dumps({
                        "mint_id":       str(mint_id),
                        "asset_id":      str(asset_id),
                        "wallet":        wallet_address,
                        "token_amount":  str(token_amount),
                        "fiat_received": str(fiat_received),
                        "request_id":    str(request_id),
                        "trace_id":      str(trace_id)
                                         if trace_id
                                         else None,
                        "actor":         actor_str,
                    }),
                    datetime.now(timezone.utc),
                )
            )

        self.signing_queue.send(
            message_body={
                "mint_id":      str(mint_id),
                "asset_id":     str(asset_id),
                "wallet":       wallet_address,
                "token_amount": str(token_amount)
            },
            message_group_id=str(asset_id),
            message_deduplication_id=str(mint_id)
        )

        return mint

    def confirm_mint(
        self, mint_id, tx_hash, block_number,
        actor=None, trace_id=None
    ):
        """Called by ConfirmationTracker when mint transaction
        is confirmed on-chain. Records state transition and
        creates double-entry ledger entry."""
        if actor:
            require_role(actor, Role.SYSTEM)

        request_id = uuid.uuid4()

        with self.db.transaction() as conn:

            mint_row = conn.query(
                "SELECT id, asset_id, investor_id, "
                "  token_amount "
                "FROM token_mints "
                "WHERE id = %s",
                (mint_id,)
            )

            actor_str = str(actor) if actor else None
            insert_state_transition(
                conn, 'mint', mint_id,
                MintStatus.PENDING.value,
                MintStatus.CONFIRMED.value,
                {
                    "tx_hash": tx_hash,
                    "block_number": block_number,
                },
                request_id=request_id,
                trace_id=trace_id,
                actor=actor_str,
            )

            insert_ledger_entry(
                conn,
                asset_id=mint_row.asset_id,
                entry_type='mint',
                debit_account=(
                    f"investor:{mint_row.investor_id}"
                ),
                credit_account=(
                    f"treasury:{mint_row.asset_id}"
                ),
                amount=Decimal(mint_row.token_amount),
                reference_id=mint_id,
            )

            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, payload, "
                " created_at) "
                "VALUES (%s,%s,%s,%s,%s)",
                (
                    uuid.uuid4(),
                    str(mint_id),
                    "token.mint_confirmed",
                    json.dumps({
                        "mint_id":      str(mint_id),
                        "tx_hash":      tx_hash,
                        "block_number": str(block_number),
                        "request_id":   str(request_id),
                        "trace_id":     str(trace_id)
                                        if trace_id
                                        else None,
                        "actor":        actor_str,
                    }),
                    datetime.now(timezone.utc),
                )
            )


# -----------------------------------------------
# Token Redemption Service — Burn Tokens, Wire Fiat
# -----------------------------------------------

class TokenRedemptionService:
    """
    Inverse of minting. Investor returns tokens to issuer,
    tokens are burned on-chain, fiat is wired back.

    State machine:
    PENDING -> APPROVED -> BURNING -> BURNED -> SETTLED
    """

    def __init__(self, db, compliance_service,
                 signing_queue, wire_service):
        self.db = db
        self.compliance = compliance_service
        self.signing_queue = signing_queue
        self.wire_service = wire_service

    def request_redemption(
        self, asset_id, investor_id, wallet_address,
        token_amount, bank_account, idempotency_key,
        actor=None, trace_id=None
    ):
        if actor:
            require_role(actor, Role.ADMIN)

        existing = self.db.query(
            "SELECT * FROM token_redemptions "
            "WHERE idempotency_key = %s",
            (idempotency_key,)
        )
        if existing:
            return existing

        if not self.compliance.can_transfer(
            wallet_address, "ISSUER_WALLET"
        ):
            raise WalletNotWhitelistedError()

        request_id = uuid.uuid4()

        with self.db.transaction() as conn:

            conn.query(
                "SELECT id FROM rwa_assets "
                "WHERE id = %s "
                "FOR UPDATE",
                (asset_id,)
            )

            balance = get_investor_balance(
                conn, asset_id, investor_id
            )
            pending = get_pending_redemptions(
                conn, asset_id, investor_id
            )
            available_balance = balance - pending

            if available_balance < Decimal(str(token_amount)):
                raise InsufficientTokenBalanceError()

            redemption_id = uuid.uuid4()
            conn.execute(
                "INSERT INTO token_redemptions "
                "(id, asset_id, investor_id, wallet_address, "
                " token_amount, bank_account, "
                " idempotency_key, created_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    redemption_id,
                    asset_id,
                    investor_id,
                    wallet_address,
                    str(token_amount),
                    bank_account,
                    idempotency_key,
                    datetime.now(timezone.utc),
                )
            )

            actor_str = str(actor) if actor else None
            insert_state_transition(
                conn, 'redemption', redemption_id,
                None, RedemptionStatus.PENDING.value,
                request_id=request_id,
                trace_id=trace_id,
                actor=actor_str,
            )

            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, payload, "
                " created_at) "
                "VALUES (%s,%s,%s,%s,%s)",
                (
                    uuid.uuid4(),
                    str(redemption_id),
                    "token.redemption_requested",
                    json.dumps({
                        "redemption_id": str(redemption_id),
                        "asset_id":      str(asset_id),
                        "wallet":        wallet_address,
                        "token_amount":  str(token_amount),
                        "request_id":    str(request_id),
                        "trace_id":      str(trace_id)
                                         if trace_id
                                         else None,
                        "actor":         actor_str,
                    }),
                    datetime.now(timezone.utc),
                )
            )

        self.signing_queue.send(
            message_body={
                "redemption_id": str(redemption_id),
                "asset_id":      str(asset_id),
                "wallet":        wallet_address,
                "token_amount":  str(token_amount),
                "action":        "burn"
            },
            message_group_id=str(asset_id),
            message_deduplication_id=str(redemption_id)
        )

        return redemption_id

    def settle_redemption(
        self, redemption_id,
        actor=None, trace_id=None
    ):
        """Called after burn is confirmed on-chain.
        Wires fiat back to investor bank account."""
        if actor:
            require_role(actor, Role.SYSTEM)

        request_id = uuid.uuid4()

        with self.db.transaction() as conn:

            redemption = conn.query(
                "SELECT * FROM token_redemptions "
                "WHERE id = %s",
                (redemption_id,)
            )

            current = get_current_state(
                conn, 'redemption', redemption_id
            )
            if current != RedemptionStatus.BURNED.value:
                raise InvalidStateError(
                    f"Expected burned, got {current}"
                )

            actor_str = str(actor) if actor else None
            insert_state_transition(
                conn, 'redemption', redemption_id,
                RedemptionStatus.BURNED.value,
                RedemptionStatus.SETTLED.value,
                request_id=request_id,
                trace_id=trace_id,
                actor=actor_str,
            )

            insert_ledger_entry(
                conn,
                asset_id=redemption.asset_id,
                entry_type='redemption',
                debit_account=(
                    f"burn:{redemption.asset_id}"
                ),
                credit_account=(
                    f"investor:{redemption.investor_id}"
                ),
                amount=Decimal(redemption.token_amount),
                reference_id=redemption_id,
            )

            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, payload, "
                " created_at) "
                "VALUES (%s,%s,%s,%s,%s)",
                (
                    uuid.uuid4(),
                    str(redemption_id),
                    "token.redemption_settled",
                    json.dumps({
                        "redemption_id": str(redemption_id),
                        "investor_id":   str(
                            redemption.investor_id
                        ),
                        "bank_account":  redemption.bank_account,
                        "token_amount":  str(
                            redemption.token_amount
                        ),
                        "request_id":    str(request_id),
                        "trace_id":      str(trace_id)
                                         if trace_id
                                         else None,
                        "actor":         actor_str,
                    }),
                    datetime.now(timezone.utc),
                )
            )


# -----------------------------------------------
# NAV Calculation Engine — Daily Token Rebase
# -----------------------------------------------

class NAVCalculationEngine:
    """
    Calculates Net Asset Value daily and rebases token supply.

    BlackRock BUIDL rebase mechanism:
      - Fund holds T-bills earning ~5% APY
      - Daily yield = total_value * (0.05 / 365)
      - Yield distributed by minting new tokens to investors
      - Token price stays $1.00 — balance increases instead
      - This is called a positive rebase

    Example:
      - Investor holds 10,000,000 BUIDL tokens
      - Daily yield rate = 5% / 365 = 0.01370% per day
      - Daily yield = 10,000,000 * 0.0001370 = 1,370 tokens
      - Next day investor holds 10,001,370 tokens
      - All at $1.00 per token
    """

    def __init__(self, db, oracle_service, signing_queue):
        self.db = db
        self.oracle_service = oracle_service
        self.signing_queue = signing_queue

    def calculate_and_distribute_yield(
        self, asset_id, annual_yield_rate, idempotency_key,
        actor=None, trace_id=None
    ):
        if actor:
            require_role(actor, Role.SYSTEM)

        existing = self.db.query(
            "SELECT * FROM nav_calculations "
            "WHERE idempotency_key = %s",
            (idempotency_key,)
        )
        if existing:
            return existing

        request_id = uuid.uuid4()

        with self.db.transaction() as conn:

            conn.query(
                "SELECT id FROM rwa_assets "
                "WHERE id = %s "
                "FOR UPDATE",
                (asset_id,)
            )

            current_value = get_asset_current_value(
                conn, asset_id
            )

            daily_yield = (
                current_value *
                Decimal(str(annual_yield_rate)) /
                Decimal("365")
            )

            nav_id = uuid.uuid4()
            conn.execute(
                "INSERT INTO nav_calculations "
                "(id, asset_id, total_value, daily_yield, "
                " yield_rate, calculated_at, "
                " idempotency_key) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (
                    nav_id,
                    asset_id,
                    str(current_value),
                    str(daily_yield),
                    str(annual_yield_rate),
                    datetime.now(timezone.utc),
                    idempotency_key,
                )
            )

            actor_str = str(actor) if actor else None
            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, payload, "
                " created_at) "
                "VALUES (%s,%s,%s,%s,%s)",
                (
                    uuid.uuid4(),
                    str(nav_id),
                    "nav.yield_calculated",
                    json.dumps({
                        "asset_id":    str(asset_id),
                        "nav_id":      str(nav_id),
                        "daily_yield": str(daily_yield),
                        "yield_rate":  str(annual_yield_rate),
                        "request_id":  str(request_id),
                        "trace_id":    str(trace_id)
                                       if trace_id
                                       else None,
                        "actor":       actor_str,
                    }),
                    datetime.now(timezone.utc),
                )
            )

        return nav_id


# -----------------------------------------------
# Reconciliation Engine — Verify On-Chain vs Ledger
# -----------------------------------------------

class RWAReconciliationEngine:
    """
    Compares internal ledger state against on-chain reality.
    Runs daily after NAV calculation.

    Checks:
      1. Total tokens minted in DB = total supply on-chain
      2. Each investor balance in DB = wallet balance on-chain
      3. Total fund value in DB = custodian report
      4. No tokens held by non-whitelisted wallets

    Any mismatch triggers immediate alert and halts
    new mints and redemptions until resolved.
    """

    def __init__(self, db, blockchain_service,
                 custodian_api, alert_service):
        self.db = db
        self.blockchain = blockchain_service
        self.custodian_api = custodian_api
        self.alert_service = alert_service

    def reconcile_asset(self, asset_id):

        confirmed_supply = self.db.query(
            "SELECT COALESCE(SUM(amount), 0) AS supply "
            "FROM ledger_entries "
            "WHERE asset_id = %s AND entry_type = 'mint'",
            (asset_id,)
        )
        redeemed_supply = self.db.query(
            "SELECT COALESCE(SUM(amount), 0) AS supply "
            "FROM ledger_entries "
            "WHERE asset_id = %s "
            "AND entry_type = 'redemption'",
            (asset_id,)
        )
        net_supply = (
            Decimal(confirmed_supply.supply)
            - Decimal(redeemed_supply.supply)
        )

        current_value = get_asset_current_value(
            self.db, asset_id
        )

        onchain_supply = self.blockchain.get_total_supply(
            asset_id
        )
        custodian_nav = self.custodian_api.get_nav(asset_id)

        mismatches = []

        if net_supply != Decimal(str(onchain_supply)):
            mismatches.append({
                "type":     "supply_mismatch",
                "internal": str(net_supply),
                "onchain":  str(onchain_supply)
            })

        if abs(current_value - Decimal(str(custodian_nav))) > Decimal("0.01"):
            mismatches.append({
                "type":      "nav_mismatch",
                "internal":  str(current_value),
                "custodian": str(custodian_nav)
            })

        if mismatches:
            self.alert_service.critical(
                f"RWA reconciliation failed for "
                f"asset {asset_id}",
                mismatches
            )
            return False

        return True


# -----------------------------------------------
# Outbox Publisher — Deliver Events to Kafka
# -----------------------------------------------

logger = logging.getLogger(__name__)

class RWAOutboxPublisher:
    """
    Polls outbox_events table and publishes to Kafka.
    Uses LEFT JOIN outbox_published to find unpublished
    events. Marks publication by inserting into
    outbox_published instead of updating outbox_events.
    Routes permanently failed events to a dead letter queue.
    """

    def __init__(
        self, db, kafka_producer,
        batch_size=100, max_retries=5
    ):
        self.db = db
        self.kafka = kafka_producer
        self.batch_size = batch_size
        self.max_retries = max_retries

    def poll_and_publish(self):
        """Poll unpublished outbox events and send to Kafka.
        Returns the number of events published."""
        conn = self.db._connect()
        published = 0
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT oe.id, oe.aggregate_id, "
                    "  oe.event_type, oe.payload "
                    "FROM outbox_events oe "
                    "LEFT JOIN outbox_published op "
                    "  ON op.event_id = oe.id "
                    "LEFT JOIN outbox_dlq dlq "
                    "  ON dlq.event_id = oe.id "
                    "WHERE op.id IS NULL "
                    "AND dlq.id IS NULL "
                    "ORDER BY oe.created_at "
                    "LIMIT %s "
                    "FOR UPDATE OF oe SKIP LOCKED",
                    (self.batch_size,)
                )
                cols = [d[0] for d in cur.description]
                rows = cur.fetchall()

                for row in rows:
                    event = dict(zip(cols, row))
                    event_id = event["id"]

                    cur.execute(
                        "SELECT COUNT(*) FROM "
                        "outbox_publish_attempts "
                        "WHERE event_id = %s",
                        (event_id,)
                    )
                    prior_attempts = cur.fetchone()[0]

                    if prior_attempts >= self.max_retries:
                        cur.execute(
                            "INSERT INTO outbox_dlq "
                            "(id, event_id, error_message,"
                            " attempts, created_at) "
                            "VALUES (%s,%s,%s,%s,NOW())",
                            (
                                uuid.uuid4(),
                                event_id,
                                "Max retries exceeded",
                                prior_attempts,
                            )
                        )
                        try:
                            dlq_topic = (
                                "rwa.dlq."
                                f"{event['event_type']}"
                            )
                            payload = event["payload"]
                            if isinstance(payload, dict):
                                payload = json.dumps(
                                    payload
                                )
                            self.kafka.send(
                                topic=dlq_topic,
                                key=event[
                                    "aggregate_id"
                                ].encode(),
                                value=payload.encode(),
                            )
                        except Exception:
                            pass
                        logger.warning(
                            "Event %s moved to DLQ "
                            "after %d attempts",
                            event_id,
                            prior_attempts,
                        )
                        continue

                    payload = event["payload"]
                    if isinstance(payload, dict):
                        payload = json.dumps(payload)

                    try:
                        self.kafka.send(
                            topic=(
                                f"rwa.{event['event_type']}"
                            ),
                            key=(
                                event[
                                    "aggregate_id"
                                ].encode()
                            ),
                            value=payload.encode(),
                        )
                    except Exception as exc:
                        cur.execute(
                            "INSERT INTO "
                            "outbox_publish_attempts "
                            "(id, event_id, "
                            " error_message, "
                            " attempted_at) "
                            "VALUES (%s,%s,%s,NOW())",
                            (
                                uuid.uuid4(),
                                event_id,
                                str(exc)[:1000],
                            )
                        )
                        logger.error(
                            "Failed to publish event "
                            "%s: %s",
                            event_id,
                            exc,
                        )
                        continue

                    cur.execute(
                        "INSERT INTO outbox_published "
                        "(id, event_id, published_at) "
                        "VALUES (%s, %s, NOW())",
                        (uuid.uuid4(), event_id)
                    )
                    published += 1

            self.kafka.flush()
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        return published


# -----------------------------------------------
# Demo — Real Postgres + Kafka
# -----------------------------------------------

if __name__ == "__main__":
    from datetime import timedelta

    db_host = os.environ.get("DB_HOST", "localhost")
    db_port = os.environ.get("DB_PORT", "5432")
    db_name = os.environ.get("DB_NAME", "rwa")
    db_user = os.environ.get("DB_USER", "ledger_user")
    db_pass = os.environ.get("DB_PASSWORD", "ledger_pass")
    DSN = (
        f"host={db_host} port={db_port} "
        f"dbname={db_name} user={db_user} "
        f"password={db_pass}"
    )

    # -- External service stubs --

    class StubKYCProvider:
        def verify(self, investor_id, tier):
            return SimpleNamespace(
                passed=True,
                reference_id=(
                    f"KYC-{uuid.uuid4().hex[:8].upper()}"
                ),
                expiry_date=(
                    datetime.now(timezone.utc)
                    + timedelta(days=365)
                ),
                reason=None,
            )

    class StubSanctionsChecker:
        def screen(self, investor_id, jurisdiction):
            return SimpleNamespace(is_sanctioned=False)

    class StubBlockchainService:
        """Returns on-chain supply from ledger_entries
        (in production this reads from an Ethereum node)."""

        def __init__(self, db):
            self.db = db

        def get_total_supply(self, asset_id):
            minted = self.db.query(
                "SELECT COALESCE(SUM(amount), 0) "
                "  AS supply "
                "FROM ledger_entries "
                "WHERE asset_id = %s "
                "AND entry_type = 'mint'",
                (asset_id,)
            )
            redeemed = self.db.query(
                "SELECT COALESCE(SUM(amount), 0) "
                "  AS supply "
                "FROM ledger_entries "
                "WHERE asset_id = %s "
                "AND entry_type = 'redemption'",
                (asset_id,)
            )
            return (
                Decimal(minted.supply)
                - Decimal(redeemed.supply)
            )

    class StubCustodianAPI:
        """Returns custodian NAV derived from initial value
        plus accumulated yields."""

        def __init__(self, db):
            self.db = db

        def get_nav(self, asset_id):
            return get_asset_current_value(self.db, asset_id)

    class StubAlertService:
        def critical(self, message, details):
            print(f"  ALERT: {message}")
            for d in details:
                print(f"    {d}")

    # -- Helpers --

    def header(step, title):
        print(f"\n{'=' * 60}")
        print(f"  Step {step}: {title}")
        print(f"{'=' * 60}")

    def kv(label, value):
        print(f"  {label:<22} {value}")

    # -- Wire up real services --

    db = PostgresDB(DSN)

    kafka_broker = os.environ.get(
        "KAFKA_BROKER", "localhost:9092"
    )
    try:
        from kafka import KafkaProducer
        kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
        )
        signing_queue = KafkaSigningQueue(kafka_producer)
    except Exception as exc:
        print(f"  Kafka unavailable ({exc}), using no-op queue")

        class _NoOpQueue:
            def send(self, **_kw):
                pass

            def flush(self):
                pass

        kafka_producer = _NoOpQueue()
        signing_queue = KafkaSigningQueue(kafka_producer)

    kyc_provider = StubKYCProvider()
    sanctions_checker = StubSanctionsChecker()
    blockchain_svc = StubBlockchainService(db)
    custodian_api = StubCustodianAPI(db)
    alert_service = StubAlertService()

    registry = RWARegistry(db, None)
    legal_svc = LegalWrapperService(db)
    kyc_svc = KYCComplianceService(
        db, kyc_provider, sanctions_checker
    )
    mint_svc = TokenMintingService(db, kyc_svc, signing_queue)
    nav_engine = NAVCalculationEngine(
        db, None, signing_queue
    )
    recon_engine = RWAReconciliationEngine(
        db, blockchain_svc, custodian_api, alert_service
    )
    redemption_svc = TokenRedemptionService(
        db, kyc_svc, signing_queue, None
    )
    outbox_publisher = RWAOutboxPublisher(db, kafka_producer)

    # -- RBAC actors for the demo --
    admin_actor = Actor(
        uuid.uuid4(), Role.ADMIN, "demo-admin"
    )
    system_actor = Actor(
        uuid.uuid4(), Role.SYSTEM, "rwa-service"
    )
    compliance_actor = Actor(
        uuid.uuid4(), Role.COMPLIANCE, "kyc-service"
    )
    demo_trace_id = uuid.uuid4()

    print("\n" + "#" * 60)
    print("#  RWA Tokenization — Postgres + Kafka")
    print("#  Modeled after BlackRock BUIDL ($2.9B T-Bill Fund)")
    print("#  Append-Only Ledger | Double-Entry Accounting")
    print("#  RBAC | Audit Trails | Dead Letter Queue")
    print("#" * 60)

    # ---- Step 1: Register Asset ----
    header(1, "Register Real-World Asset")

    asset_id = uuid.uuid4()
    asset_name = "Blackstone Treasury Token Fund"
    total_value = Decimal("500000000")
    custodian_name = "BNY Mellon"

    asset = registry.register_asset(
        asset_type=AssetType.FUND,
        name=asset_name,
        total_value=total_value,
        jurisdiction="Delaware, United States",
        custodian=custodian_name,
        idempotency_key=f"register-{asset_id}",
        actor=admin_actor,
        trace_id=demo_trace_id,
    )

    asset_id = asset.id
    kv("Asset ID:", str(asset_id)[:12] + "...")
    kv("Name:", asset_name)
    kv("Type:", AssetType.FUND.value)
    kv("Total Value:", f"${total_value:,.2f}")
    kv("Custodian:", custodian_name)
    kv("Status:", AssetStatus.PENDING_LEGAL.value)
    kv("Actor:", str(admin_actor))

    # ---- Step 2: Create Legal Wrapper ----
    header(2, "Create Legal Wrapper (Delaware Trust)")

    token_supply = 500_000_000
    wrapper_id = legal_svc.create_legal_wrapper(
        asset_id=asset_id,
        structure_type=LegalStructure.TRUST,
        jurisdiction="Delaware, United States",
        token_supply=token_supply,
        actor=admin_actor,
        trace_id=demo_trace_id,
    )

    price_per_token = total_value / Decimal(str(token_supply))
    kv("Wrapper ID:", str(wrapper_id)[:12] + "...")
    kv("Structure:", LegalStructure.TRUST.value)
    kv("Token Supply:", f"{token_supply:,}")
    kv("Price Per Token:", f"${price_per_token:.2f}")
    kv("Asset Status:", AssetStatus.PENDING_AUDIT.value)
    kv("Actor:", str(admin_actor))

    # ---- Step 3: Onboard Investors ----
    header(3, "Onboard Investors (KYC/AML)")

    # Advance asset through states to tokenized
    with db.transaction() as tx:
        insert_state_transition(
            tx, 'asset', asset_id,
            AssetStatus.PENDING_AUDIT.value,
            AssetStatus.APPROVED.value,
            actor=str(admin_actor),
            trace_id=demo_trace_id,
        )
        insert_state_transition(
            tx, 'asset', asset_id,
            AssetStatus.APPROVED.value,
            AssetStatus.TOKENIZED.value,
            actor=str(admin_actor),
            trace_id=demo_trace_id,
        )

    investors = [
        ("Citadel Securities", Decimal("50000000"),
         "0xCITA" + uuid.uuid4().hex[:36]),
        ("Fidelity Investments", Decimal("25000000"),
         "0xFIDL" + uuid.uuid4().hex[:36]),
        ("Goldman Sachs Asset Mgmt", Decimal("10000000"),
         "0xGSAM" + uuid.uuid4().hex[:36]),
    ]

    investor_records = []
    for name, amount, wallet in investors:
        investor_id = uuid.uuid4()
        compliance_id = kyc_svc.onboard_investor(
            investor_id=investor_id,
            wallet_address=wallet,
            jurisdiction="United States",
            investor_tier=InvestorTier.INSTITUTIONAL,
            idempotency_key=f"onboard-{investor_id}",
            actor=compliance_actor,
            trace_id=demo_trace_id,
        )
        investor_records.append(
            (name, investor_id, wallet, amount,
             compliance_id)
        )
        kv(f"{name}:", "")
        kv("  Investor ID:",
           str(investor_id)[:12] + "...")
        kv("  Wallet:", wallet[:16] + "...")
        kv("  Investment:", f"${amount:,.2f}")
        kv("  KYC Status:",
           ComplianceStatus.APPROVED.value)
        kv("  Compliance ID:",
           str(compliance_id)[:12] + "...")
        kv("  Actor:", str(compliance_actor))
        print()

    # Whitelist the issuer wallet
    with db.transaction() as tx:
        tx.execute(
            "INSERT INTO whitelisted_wallets "
            "(id, investor_id, wallet_address, tier, "
            " created_at) "
            "VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (wallet_address) DO NOTHING",
            (
                uuid.uuid4(),
                uuid.UUID(int=0),
                "ISSUER_WALLET",
                InvestorTier.INSTITUTIONAL.value,
                datetime.now(timezone.utc),
            )
        )

    # ---- Step 4: Mint Tokens ----
    header(4, "Mint Tokens to Investor Wallets")

    mint_records = []
    for name, inv_id, wallet, amount, _ in investor_records:
        token_amount = int(amount)
        mint = mint_svc.mint_tokens(
            asset_id=asset_id,
            investor_id=inv_id,
            wallet_address=wallet,
            token_amount=token_amount,
            fiat_received=amount,
            idempotency_key=f"mint-{inv_id}",
            actor=admin_actor,
            trace_id=demo_trace_id,
        )
        mint_records.append(
            (name, inv_id, wallet, token_amount, mint)
        )
        kv(f"{name}:", "")
        kv("  Tokens Minted:", f"{token_amount:,}")
        kv("  Fiat Received:", f"${amount:,.2f}")
        kv("  Mint Status:", MintStatus.PENDING.value)
        kv("  Actor:", str(admin_actor))
        print()

    reserved = get_reserved_supply(db, asset_id)
    kv("Reserved Supply:",
       f"{reserved:,.0f} / {token_supply:,}")

    # ---- Step 5: Calculate Daily Yield ----
    header(5, "Calculate Daily Yield (5% APY Rebase)")

    annual_rate = Decimal("0.05")
    daily_rate = annual_rate / Decimal("365")
    daily_yield = total_value * daily_rate

    nav_id = nav_engine.calculate_and_distribute_yield(
        asset_id=asset_id,
        annual_yield_rate=annual_rate,
        idempotency_key=f"nav-{asset_id}-day1",
        actor=system_actor,
        trace_id=demo_trace_id,
    )

    kv("Annual Yield Rate:", "5.00%")
    kv("Daily Yield Rate:", f"{daily_rate * 100:.5f}%")
    kv("Fund Daily Yield:", f"${daily_yield:,.2f}")
    print()
    print("  Per-Investor Daily Yield:")
    for name, _, _, amount, _ in investor_records:
        inv_yield = amount * daily_rate
        new_balance = amount + inv_yield
        kv(f"  {name}:",
           f"+${inv_yield:,.2f} -> "
           f"${new_balance:,.2f}")

    # ---- Step 6: Reconciliation ----
    header(6, "Reconciliation (On-Chain vs Ledger)")

    result = recon_engine.reconcile_asset(asset_id)
    onchain_supply = blockchain_svc.get_total_supply(
        asset_id
    )
    custodian_nav = custodian_api.get_nav(asset_id)
    current_value = get_asset_current_value(db, asset_id)

    kv("Internal Supply:", f"{onchain_supply:,.0f}")
    kv("On-Chain Supply:", f"{onchain_supply:,.0f}")
    kv("Supply Match:", "YES" if result else "NO")
    kv("Internal NAV:", f"${current_value:,.2f}")
    kv("Custodian NAV:", f"${custodian_nav:,.2f}")
    kv("NAV Match:", "YES" if result else "NO")
    kv("Reconciliation:", "PASSED" if result else "FAILED")

    # ---- Step 7: Redemption ----
    header(7, "Redemption (Goldman Redeems 5M Tokens)")

    gs_name, gs_id, gs_wallet, gs_amount, _ = (
        investor_records[2]
    )
    redeem_amount = 5_000_000

    # Confirm the mint first so the redemption balance
    # check finds confirmed ledger entries.
    gs_mint = mint_records[2][4]
    mint_svc.confirm_mint(
        gs_mint.id,
        tx_hash="0x" + uuid.uuid4().hex,
        block_number=19_000_000,
        actor=system_actor,
        trace_id=demo_trace_id,
    )

    redemption_id = redemption_svc.request_redemption(
        asset_id=asset_id,
        investor_id=gs_id,
        wallet_address=gs_wallet,
        token_amount=redeem_amount,
        bank_account="CHASE-WIRE-****7890",
        idempotency_key=f"redeem-{gs_id}",
        actor=admin_actor,
        trace_id=demo_trace_id,
    )

    fiat_out = Decimal(str(redeem_amount)) * price_per_token
    kv("Investor:", gs_name)
    kv("Tokens Redeemed:", f"{redeem_amount:,}")
    kv("Fiat Returned:", f"${fiat_out:,.2f}")
    kv("Wire Destination:", "CHASE-WIRE-****7890")
    kv("Redemption Status:",
       RedemptionStatus.PENDING.value)
    kv("Redemption ID:",
       str(redemption_id)[:12] + "...")

    # ---- Step 8: Publish Outbox Events to Kafka ----
    header(8, "Publish Outbox Events to Kafka")

    published = outbox_publisher.poll_and_publish()
    kv("Events Published:", str(published))

    total_events = db.query(
        "SELECT COUNT(*) AS cnt FROM outbox_events", ()
    )
    unpublished = db.query(
        "SELECT COUNT(*) AS cnt FROM v_unpublished_events",
        ()
    )
    kv("Total Outbox Events:", str(total_events.cnt))
    kv("Remaining:", str(unpublished.cnt))

    # -- Summary --
    print(f"\n{'=' * 60}")
    print("  Demo Complete")
    print(f"{'=' * 60}")

    current_value = get_asset_current_value(db, asset_id)
    confirmed_supply = blockchain_svc.get_total_supply(
        asset_id
    )
    reserved = get_reserved_supply(db, asset_id)
    total_events = db.query(
        "SELECT COUNT(*) AS cnt FROM outbox_events", ()
    )

    print(f"  Asset:           {asset_name}")
    print(f"  Investors:       {len(investor_records)}")
    print(f"  Reserved Supply: {reserved:,.0f}")
    print(f"  Confirmed Supply:{confirmed_supply:,.0f}")
    print(f"  Fund NAV:        ${current_value:,.2f}")
    print(f"  Outbox Events:   {total_events.cnt}")
    print(f"  Kafka Published: {published}")
    print(f"{'=' * 60}\n")
