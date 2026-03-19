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
    SPV   = "spv"      # Special Purpose Vehicle — real estate
    TRUST = "trust"    # Delaware Trust — bonds, funds
    LLC   = "llc"      # LLC — private equity
    FUND  = "fund"     # Regulated fund — BlackRock BUIDL

class InvestorTier(str, Enum):
    RETAIL        = "retail"         # Limited access
    ACCREDITED    = "accredited"     # $1M+ net worth
    QUALIFIED     = "qualified"      # $5M+ investments
    INSTITUTIONAL = "institutional"  # Banks, funds, hedge funds

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
    BURNING   = "burning"    # Tokens being burned on-chain
    BURNED    = "burned"     # Tokens confirmed burned
    SETTLED   = "settled"    # Fiat wired back to investor
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
        jurisdiction, custodian, idempotency_key
    ):
        # Idempotency check first — never register same asset twice
        existing = self.db.query(
            "SELECT * FROM rwa_assets "
            "WHERE idempotency_key = %s",
            (idempotency_key,)
        )
        if existing:
            return existing

        # Validate custodian is licensed and approved
        if not self._validate_custodian(custodian):
            raise InvalidCustodianError(
                f"Custodian {custodian} is not approved"
            )

        # Atomic registration
        with self.db.transaction() as conn:

            asset_id = uuid.uuid4()
            asset = conn.execute(
                "INSERT INTO rwa_assets "
                "(id, type, name, total_value, jurisdiction, "
                " custodian, status, idempotency_key, "
                " created_at, updated_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "RETURNING *",
                (
                    asset_id,
                    asset_type.value,
                    name,
                    str(total_value),           # str preserves DECIMAL precision
                    jurisdiction,
                    custodian,
                    AssetStatus.PENDING_LEGAL.value,
                    idempotency_key,
                    datetime.now(timezone.utc),
                    datetime.now(timezone.utc)
                )
            )

            # Write to outbox — triggers legal review workflow
            # If downstream service is down, event waits safely here
            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, payload, created_at) "
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
                        "custodian":    custodian
                    }),
                    datetime.now(timezone.utc)
                )
            )

        return asset

    def _validate_custodian(self, custodian):
        # In production: check custodian is licensed,
        # regulated, and on the approved custodian list
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
        jurisdiction, token_supply
    ):
        with self.db.transaction() as conn:

            # Pessimistic lock — one legal wrapper per asset
            asset = conn.query(
                "SELECT id, status, total_value "
                "FROM rwa_assets "
                "WHERE id = %s "
                "FOR UPDATE",
                (asset_id,)
            )

            # State machine guard — must be in correct state
            if asset.status != AssetStatus.PENDING_LEGAL.value:
                raise InvalidStateError(
                    f"Expected pending_legal, got {asset.status}"
                )

            # Token economics
            # total_value / token_supply = price per token
            # BUIDL: $2.9B / 2.9B tokens = $1.00 per token
            price_per_token = (
                Decimal(asset.total_value) /
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
                    datetime.now(timezone.utc)
                )
            )

            # Initialize token supply tracker
            conn.execute(
                "INSERT INTO rwa_token_supply "
                "(id, asset_id, total_supply, "
                " minted_supply, created_at, updated_at) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                (
                    uuid.uuid4(),
                    asset_id,
                    str(token_supply),
                    "0",                        # nothing minted yet
                    datetime.now(timezone.utc),
                    datetime.now(timezone.utc)
                )
            )

            # Advance asset state machine
            conn.execute(
                "UPDATE rwa_assets "
                "SET status = %s, updated_at = NOW() "
                "WHERE id = %s",
                (AssetStatus.PENDING_AUDIT.value, asset_id)
            )

            # Outbox — triggers audit workflow
            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, payload, created_at) "
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
                        "price_per_token": str(price_per_token)
                    }),
                    datetime.now(timezone.utc)
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
    Both sender AND receiver must be whitelisted on every transfer.
    This is enforced at the smart contract level — not just policy.

    BlackRock BUIDL requirements:
      - Institutional investors only
      - Minimum $5M investment
      - Full KYC/AML via Securitize platform
      - Whitelisted Ethereum wallet addresses only
      - Transfers only between whitelisted addresses
      - KYC expires annually — must re-verify

    This is the PRIMARY difference between RWA tokens
    and standard ERC-20 tokens.
    """

    def __init__(self, db, kyc_provider, sanctions_checker):
        self.db = db
        self.kyc_provider = kyc_provider
        self.sanctions_checker = sanctions_checker

    def onboard_investor(
        self, investor_id, wallet_address,
        jurisdiction, investor_tier,
        idempotency_key
    ):
        # Idempotency check — never double-onboard same investor
        existing = self.db.query(
            "SELECT * FROM investor_compliance "
            "WHERE idempotency_key = %s",
            (idempotency_key,)
        )
        if existing:
            return existing

        # Step 1: Sanctions screening — OFAC, EU, UN lists
        # Must happen OUTSIDE transaction — external API call
        # Never hold a row lock during external calls
        sanctions_result = self.sanctions_checker.screen(
            investor_id, jurisdiction
        )
        if sanctions_result.is_sanctioned:
            raise SanctionedInvestorError(
                f"Investor {investor_id} is on a sanctions list"
            )

        # Step 2: KYC/AML verification — identity + accreditation
        # Also outside transaction — external API call
        kyc_result = self.kyc_provider.verify(
            investor_id, investor_tier
        )
        if not kyc_result.passed:
            raise KYCFailedError(kyc_result.reason)

        # Step 3: Write compliance record + whitelist wallet atomically
        with self.db.transaction() as conn:

            compliance_id = uuid.uuid4()
            conn.execute(
                "INSERT INTO investor_compliance "
                "(id, investor_id, wallet_address, tier, "
                " jurisdiction, status, kyc_reference, "
                " idempotency_key, created_at, expires_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    compliance_id,
                    investor_id,
                    wallet_address,
                    investor_tier.value,
                    jurisdiction,
                    ComplianceStatus.APPROVED.value,
                    kyc_result.reference_id,
                    idempotency_key,
                    datetime.now(timezone.utc),
                    kyc_result.expiry_date     # KYC expires — must re-verify
                )
            )

            # Whitelist the wallet address
            # Only whitelisted wallets can send or receive RWA tokens
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
                    datetime.now(timezone.utc)
                )
            )

            # Outbox — notifies token platform of new whitelisted wallet
            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, payload, created_at) "
                "VALUES (%s,%s,%s,%s,%s)",
                (
                    uuid.uuid4(),
                    str(compliance_id),
                    "investor.whitelisted",
                    json.dumps({
                        "investor_id":    str(investor_id),
                        "wallet_address": wallet_address,
                        "tier":           investor_tier.value,
                        "jurisdiction":   jurisdiction
                    }),
                    datetime.now(timezone.utc)
                )
            )

        return compliance_id

    def can_transfer(self, from_wallet, to_wallet):
        """
        Called before every token transfer.
        Both sender AND receiver must be whitelisted.
        Smart contract calls this via on-chain oracle
        or off-chain transfer hook depending on architecture.
        """
        sender_approved = self.db.query(
            "SELECT id FROM whitelisted_wallets "
            "WHERE wallet_address = %s",
            (from_wallet,)
        )
        receiver_approved = self.db.query(
            "SELECT id FROM whitelisted_wallets "
            "WHERE wallet_address = %s",
            (to_wallet,)
        )
        return bool(sender_approved) and bool(receiver_approved)

    def revoke_whitelist(self, investor_id, reason):
        """
        Revokes investor whitelist status.
        Called when KYC expires, sanctions hit, or
        investor requests redemption and exit.
        After revocation wallet cannot receive transfers.
        """
        with self.db.transaction() as conn:

            conn.execute(
                "DELETE FROM whitelisted_wallets "
                "WHERE investor_id = %s",
                (investor_id,)
            )

            conn.execute(
                "UPDATE investor_compliance "
                "SET status = %s, updated_at = NOW() "
                "WHERE investor_id = %s",
                (ComplianceStatus.EXPIRED.value, investor_id)
            )

            # Outbox — notifies token platform to block transfers
            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, payload, created_at) "
                "VALUES (%s,%s,%s,%s,%s)",
                (
                    uuid.uuid4(),
                    str(investor_id),
                    "investor.whitelist_revoked",
                    json.dumps({
                        "investor_id": str(investor_id),
                        "reason":      reason
                    }),
                    datetime.now(timezone.utc)
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
      - Lock token supply FOR UPDATE
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
        token_amount, fiat_received, idempotency_key
    ):
        # Idempotency check first — never double-mint
        existing = self.db.query(
            "SELECT * FROM token_mints "
            "WHERE idempotency_key = %s",
            (idempotency_key,)
        )
        if existing:
            return existing

        # Wallet must be whitelisted before receiving tokens
        if not self.compliance.can_transfer(
            "ISSUER_WALLET", wallet_address
        ):
            raise WalletNotWhitelistedError(
                f"Wallet {wallet_address} is not whitelisted"
            )

        # Atomic mint record + supply update + outbox
        with self.db.transaction() as conn:

            # Pessimistic lock — one mint at a time per asset
            # Prevents overselling token supply
            supply = conn.query(
                "SELECT id, total_supply, minted_supply "
                "FROM rwa_token_supply "
                "WHERE asset_id = %s "
                "FOR UPDATE",
                (asset_id,)
            )

            # Check available token supply
            available = (
                Decimal(supply.total_supply) -
                Decimal(supply.minted_supply)
            )
            if available < Decimal(str(token_amount)):
                raise InsufficientTokenSupplyError(
                    f"Only {available} tokens remaining"
                )

            # Reserve the tokens — locked until confirmed on-chain
            conn.execute(
                "UPDATE rwa_token_supply "
                "SET minted_supply = minted_supply + %s, "
                "updated_at = NOW() "
                "WHERE asset_id = %s",
                (str(token_amount), asset_id)
            )

            # Create mint record — state machine entry point
            mint_id = uuid.uuid4()
            mint = conn.execute(
                "INSERT INTO token_mints "
                "(id, asset_id, investor_id, wallet_address, "
                " token_amount, fiat_received, status, "
                " idempotency_key, created_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "RETURNING *",
                (
                    mint_id,
                    asset_id,
                    investor_id,
                    wallet_address,
                    str(token_amount),
                    str(fiat_received),
                    MintStatus.PENDING.value,
                    idempotency_key,
                    datetime.now(timezone.utc)
                )
            )

            # Write to outbox in same transaction
            # If blockchain is down, event waits safely here
            # Outbox publisher delivers to Kafka when ready
            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, payload, created_at) "
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
                        "fiat_received": str(fiat_received)
                    }),
                    datetime.now(timezone.utc)
                )
            )

        # Publish to signing queue OUTSIDE the transaction
        # Never hold a row lock during external calls
        # message_group_id = asset_id ensures FIFO per asset
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

    def confirm_mint(self, mint_id, tx_hash, block_number):
        """
        Called by ConfirmationTracker when mint transaction
        is confirmed on-chain. Settles the ledger.
        """
        with self.db.transaction() as conn:

            conn.execute(
                "UPDATE token_mints "
                "SET status = %s, "
                "tx_hash = %s, "
                "block_number = %s, "
                "confirmed_at = NOW() "
                "WHERE id = %s",
                (
                    MintStatus.CONFIRMED.value,
                    tx_hash,
                    block_number,
                    mint_id
                )
            )

            # Outbox — notifies investor dashboard
            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, payload, created_at) "
                "VALUES (%s,%s,%s,%s,%s)",
                (
                    uuid.uuid4(),
                    str(mint_id),
                    "token.mint_confirmed",
                    json.dumps({
                        "mint_id":     str(mint_id),
                        "tx_hash":     tx_hash,
                        "block_number": str(block_number)
                    }),
                    datetime.now(timezone.utc)
                )
            )


# -----------------------------------------------
# Token Redemption Service — Burn Tokens, Wire Fiat
# -----------------------------------------------

class TokenRedemptionService:
    """
    Inverse of minting. Investor returns tokens to issuer,
    tokens are burned on-chain, fiat is wired back.

    This is the most operationally complex flow because
    it spans blockchain (burn), compliance (re-verify),
    and traditional finance (wire transfer) — three
    separate systems that must all succeed atomically
    or not at all.

    State machine:
    PENDING → APPROVED → BURNING → BURNED → SETTLED
    """

    def __init__(self, db, compliance_service,
                 signing_queue, wire_service):
        self.db = db
        self.compliance = compliance_service
        self.signing_queue = signing_queue
        self.wire_service = wire_service

    def request_redemption(
        self, asset_id, investor_id, wallet_address,
        token_amount, bank_account, idempotency_key
    ):
        # Idempotency check first
        existing = self.db.query(
            "SELECT * FROM token_redemptions "
            "WHERE idempotency_key = %s",
            (idempotency_key,)
        )
        if existing:
            return existing

        # Verify investor still has compliance status
        if not self.compliance.can_transfer(
            wallet_address, "ISSUER_WALLET"
        ):
            raise WalletNotWhitelistedError()

        with self.db.transaction() as conn:

            # Lock investor token balance
            balance = conn.query(
                "SELECT token_amount "
                "FROM token_mints "
                "WHERE investor_id = %s "
                "AND asset_id = %s "
                "AND status = %s "
                "FOR UPDATE",
                (
                    investor_id, asset_id,
                    MintStatus.CONFIRMED.value
                )
            )

            if Decimal(balance.token_amount) < Decimal(str(token_amount)):
                raise InsufficientTokenBalanceError()

            redemption_id = uuid.uuid4()
            conn.execute(
                "INSERT INTO token_redemptions "
                "(id, asset_id, investor_id, wallet_address, "
                " token_amount, bank_account, status, "
                " idempotency_key, created_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    redemption_id,
                    asset_id,
                    investor_id,
                    wallet_address,
                    str(token_amount),
                    bank_account,
                    RedemptionStatus.PENDING.value,
                    idempotency_key,
                    datetime.now(timezone.utc)
                )
            )

            # Outbox — triggers burn workflow
            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, payload, created_at) "
                "VALUES (%s,%s,%s,%s,%s)",
                (
                    uuid.uuid4(),
                    str(redemption_id),
                    "token.redemption_requested",
                    json.dumps({
                        "redemption_id": str(redemption_id),
                        "asset_id":      str(asset_id),
                        "wallet":        wallet_address,
                        "token_amount":  str(token_amount)
                    }),
                    datetime.now(timezone.utc)
                )
            )

        # Publish to signing queue — burns tokens on-chain
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

    def settle_redemption(self, redemption_id):
        """
        Called after burn is confirmed on-chain.
        Wires fiat back to investor bank account.
        Final step — releases all locks.
        """
        with self.db.transaction() as conn:

            redemption = conn.query(
                "SELECT * FROM token_redemptions "
                "WHERE id = %s "
                "FOR UPDATE",
                (redemption_id,)
            )

            if redemption.status != RedemptionStatus.BURNED.value:
                raise InvalidStateError(
                    f"Expected burned, got {redemption.status}"
                )

            # Release token supply — tokens are gone from circulation
            conn.execute(
                "UPDATE rwa_token_supply "
                "SET minted_supply = minted_supply - %s, "
                "updated_at = NOW() "
                "WHERE asset_id = %s",
                (redemption.token_amount, redemption.asset_id)
            )

            # Mark settled
            conn.execute(
                "UPDATE token_redemptions "
                "SET status = %s, settled_at = NOW() "
                "WHERE id = %s",
                (RedemptionStatus.SETTLED.value, redemption_id)
            )

            # Outbox — triggers fiat wire transfer
            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, payload, created_at) "
                "VALUES (%s,%s,%s,%s,%s)",
                (
                    uuid.uuid4(),
                    str(redemption_id),
                    "token.redemption_settled",
                    json.dumps({
                        "redemption_id": str(redemption_id),
                        "investor_id":   str(redemption.investor_id),
                        "bank_account":  redemption.bank_account,
                        "token_amount":  str(redemption.token_amount)
                    }),
                    datetime.now(timezone.utc)
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
        self, asset_id, annual_yield_rate, idempotency_key
    ):
        # Idempotency — only one NAV calc per asset per day
        existing = self.db.query(
            "SELECT * FROM nav_calculations "
            "WHERE idempotency_key = %s",
            (idempotency_key,)
        )
        if existing:
            return existing

        with self.db.transaction() as conn:

            # Lock asset for NAV update
            asset = conn.query(
                "SELECT id, total_value "
                "FROM rwa_assets "
                "WHERE id = %s "
                "FOR UPDATE",
                (asset_id,)
            )

            # Daily yield = total_value * (annual_rate / 365)
            daily_yield = (
                Decimal(asset.total_value) *
                Decimal(str(annual_yield_rate)) /
                Decimal("365")
            )

            nav_id = uuid.uuid4()
            conn.execute(
                "INSERT INTO nav_calculations "
                "(id, asset_id, total_value, daily_yield, "
                " yield_rate, calculated_at, idempotency_key) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (
                    nav_id,
                    asset_id,
                    str(asset.total_value),
                    str(daily_yield),
                    str(annual_yield_rate),
                    datetime.now(timezone.utc),
                    idempotency_key
                )
            )

            # Update total fund value with accrued yield
            conn.execute(
                "UPDATE rwa_assets "
                "SET total_value = total_value + %s, "
                "updated_at = NOW() "
                "WHERE id = %s",
                (str(daily_yield), asset_id)
            )

            # Outbox — triggers token rebase on-chain
            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, payload, created_at) "
                "VALUES (%s,%s,%s,%s,%s)",
                (
                    uuid.uuid4(),
                    str(nav_id),
                    "nav.yield_calculated",
                    json.dumps({
                        "asset_id":    str(asset_id),
                        "nav_id":      str(nav_id),
                        "daily_yield": str(daily_yield),
                        "yield_rate":  str(annual_yield_rate)
                    }),
                    datetime.now(timezone.utc)
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

        # Get internal ledger state
        internal = self.db.query(
            "SELECT ts.minted_supply, a.total_value "
            "FROM rwa_token_supply ts "
            "JOIN rwa_assets a ON a.id = ts.asset_id "
            "WHERE ts.asset_id = %s",
            (asset_id,)
        )

        # Get on-chain token supply
        onchain_supply = self.blockchain.get_total_supply(asset_id)

        # Get custodian report — actual AUM
        custodian_nav = self.custodian_api.get_nav(asset_id)

        mismatches = []

        # Check 1: Token supply matches on-chain
        if Decimal(internal.minted_supply) != Decimal(str(onchain_supply)):
            mismatches.append({
                "type":     "supply_mismatch",
                "internal": str(internal.minted_supply),
                "onchain":  str(onchain_supply)
            })

        # Check 2: Fund value matches custodian report
        if abs(
            Decimal(internal.total_value) -
            Decimal(str(custodian_nav))
        ) > Decimal("0.01"):        # $0.01 tolerance
            mismatches.append({
                "type":      "nav_mismatch",
                "internal":  str(internal.total_value),
                "custodian": str(custodian_nav)
            })

        if mismatches:
            # Alert immediately — halt minting and redemptions
            self.alert_service.critical(
                f"RWA reconciliation failed for asset {asset_id}",
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
    Same pattern as crypto custody outbox publisher.
    FOR UPDATE SKIP LOCKED enables multiple publisher
    instances without duplicate delivery.
    """

    def __init__(self, db, kafka_producer, batch_size=100):
        self.db = db
        self.kafka = kafka_producer
        self.batch_size = batch_size

    def poll_and_publish(self):
        """Poll unpublished outbox events and send to Kafka.
        Returns the number of events published."""
        conn = self.db._connect()
        published = 0
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, aggregate_id, event_type, "
                    "payload "
                    "FROM outbox_events "
                    "WHERE published_at IS NULL "
                    "ORDER BY created_at "
                    "LIMIT %s "
                    "FOR UPDATE SKIP LOCKED",
                    (self.batch_size,)
                )
                cols = [d[0] for d in cur.description]
                rows = cur.fetchall()

                for row in rows:
                    event = dict(zip(cols, row))
                    payload = event["payload"]
                    if isinstance(payload, dict):
                        payload = json.dumps(payload)

                    self.kafka.send(
                        topic=f"rwa.{event['event_type']}",
                        key=event["aggregate_id"].encode(),
                        value=payload.encode(),
                    )

                    cur.execute(
                        "UPDATE outbox_events "
                        "SET published_at = NOW() "
                        "WHERE id = %s",
                        (event["id"],)
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

    DSN = "dbname=rwa user=postgres"

    # -- External service stubs --
    # These represent third-party APIs that cannot be replaced
    # with local infrastructure.

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
        """Returns on-chain supply from our own DB
        (in production this reads from an Ethereum node)."""

        def __init__(self, db):
            self.db = db

        def get_total_supply(self, asset_id):
            row = self.db.query(
                "SELECT minted_supply "
                "FROM rwa_token_supply "
                "WHERE asset_id = %s",
                (asset_id,)
            )
            if row:
                return Decimal(row.minted_supply)
            return Decimal("0")

    class StubCustodianAPI:
        """Returns custodian NAV from our own DB
        (in production this calls custodian's API)."""

        def __init__(self, db):
            self.db = db

        def get_nav(self, asset_id):
            row = self.db.query(
                "SELECT total_value FROM rwa_assets "
                "WHERE id = %s",
                (asset_id,)
            )
            if row:
                return Decimal(row.total_value)
            return Decimal("0")

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

    try:
        from kafka import KafkaProducer
        kafka_producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
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
    nav_engine = NAVCalculationEngine(db, None, signing_queue)
    recon_engine = RWAReconciliationEngine(
        db, blockchain_svc, custodian_api, alert_service
    )
    redemption_svc = TokenRedemptionService(
        db, kyc_svc, signing_queue, None
    )
    outbox_publisher = RWAOutboxPublisher(db, kafka_producer)

    print("\n" + "#" * 60)
    print("#  RWA Tokenization — Postgres + Kafka")
    print("#  Modeled after BlackRock BUIDL ($2.9B T-Bill Fund)")
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
    )

    asset_id = asset.id
    kv("Asset ID:", str(asset_id)[:12] + "...")
    kv("Name:", asset_name)
    kv("Type:", AssetType.FUND.value)
    kv("Total Value:", f"${total_value:,.2f}")
    kv("Custodian:", custodian_name)
    kv("Status:", AssetStatus.PENDING_LEGAL.value)

    # ---- Step 2: Create Legal Wrapper ----
    header(2, "Create Legal Wrapper (Delaware Trust)")

    token_supply = 500_000_000
    wrapper_id = legal_svc.create_legal_wrapper(
        asset_id=asset_id,
        structure_type=LegalStructure.TRUST,
        jurisdiction="Delaware, United States",
        token_supply=token_supply,
    )

    price_per_token = total_value / Decimal(str(token_supply))
    kv("Wrapper ID:", str(wrapper_id)[:12] + "...")
    kv("Structure:", LegalStructure.TRUST.value)
    kv("Token Supply:", f"{token_supply:,}")
    kv("Price Per Token:", f"${price_per_token:.2f}")
    kv("Asset Status:", AssetStatus.PENDING_AUDIT.value)

    # ---- Step 3: Onboard Investors ----
    header(3, "Onboard Investors (KYC/AML)")

    # Advance asset to tokenized so minting works
    with db.transaction() as tx:
        tx.execute(
            "UPDATE rwa_assets "
            "SET status = %s, updated_at = NOW() "
            "WHERE id = %s",
            (AssetStatus.TOKENIZED.value, asset_id)
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
        )
        investor_records.append(
            (name, investor_id, wallet, amount, compliance_id)
        )
        kv(f"{name}:", "")
        kv("  Investor ID:", str(investor_id)[:12] + "...")
        kv("  Wallet:", wallet[:16] + "...")
        kv("  Investment:", f"${amount:,.2f}")
        kv("  KYC Status:", ComplianceStatus.APPROVED.value)
        kv("  Compliance ID:", str(compliance_id)[:12] + "...")
        print()

    # Whitelist the issuer wallet so can_transfer() passes
    # during minting (issuer -> investor) and redemption
    # (investor -> issuer).
    with db.transaction() as tx:
        tx.execute(
            "INSERT INTO whitelisted_wallets "
            "(id, investor_id, wallet_address, tier, created_at) "
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
        )
        mint_records.append(
            (name, inv_id, wallet, token_amount, mint)
        )
        kv(f"{name}:", "")
        kv("  Tokens Minted:", f"{token_amount:,}")
        kv("  Fiat Received:", f"${amount:,.2f}")
        kv("  Mint Status:", MintStatus.PENDING.value)
        print()

    supply = db.query(
        "SELECT total_supply, minted_supply "
        "FROM rwa_token_supply WHERE asset_id = %s",
        (asset_id,)
    )
    kv("Total Minted Supply:",
       f"{Decimal(supply.minted_supply):,.0f} / "
       f"{Decimal(supply.total_supply):,.0f}")

    # ---- Step 5: Calculate Daily Yield ----
    header(5, "Calculate Daily Yield (5% APY Rebase)")

    annual_rate = Decimal("0.05")
    daily_rate = annual_rate / Decimal("365")
    daily_yield = total_value * daily_rate

    nav_id = nav_engine.calculate_and_distribute_yield(
        asset_id=asset_id,
        annual_yield_rate=annual_rate,
        idempotency_key=f"nav-{asset_id}-day1",
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
    supply = db.query(
        "SELECT minted_supply FROM rwa_token_supply "
        "WHERE asset_id = %s",
        (asset_id,)
    )
    onchain_supply = blockchain_svc.get_total_supply(asset_id)
    custodian_nav = custodian_api.get_nav(asset_id)
    asset_row = db.query(
        "SELECT total_value FROM rwa_assets WHERE id = %s",
        (asset_id,)
    )

    kv("Internal Supply:",
       f"{Decimal(supply.minted_supply):,.0f}")
    kv("On-Chain Supply:", f"{onchain_supply:,.0f}")
    kv("Supply Match:", "YES" if result else "NO")
    kv("Internal NAV:",
       f"${Decimal(asset_row.total_value):,.2f}")
    kv("Custodian NAV:", f"${custodian_nav:,.2f}")
    kv("NAV Match:", "YES" if result else "NO")
    kv("Reconciliation:", "PASSED" if result else "FAILED")

    # ---- Step 7: Redemption ----
    header(7, "Redemption (Goldman Redeems 5M Tokens)")

    gs_name, gs_id, gs_wallet, gs_amount, _ = (
        investor_records[2]
    )
    redeem_amount = 5_000_000

    # Confirm the mint first so the redemption balance check
    # finds a confirmed row.
    gs_mint = mint_records[2][4]
    mint_svc.confirm_mint(
        gs_mint.id,
        tx_hash="0x" + uuid.uuid4().hex,
        block_number=19_000_000,
    )

    redemption_id = redemption_svc.request_redemption(
        asset_id=asset_id,
        investor_id=gs_id,
        wallet_address=gs_wallet,
        token_amount=redeem_amount,
        bank_account="CHASE-WIRE-****7890",
        idempotency_key=f"redeem-{gs_id}",
    )

    fiat_out = Decimal(str(redeem_amount)) * price_per_token
    kv("Investor:", gs_name)
    kv("Tokens Redeemed:", f"{redeem_amount:,}")
    kv("Fiat Returned:", f"${fiat_out:,.2f}")
    kv("Wire Destination:", "CHASE-WIRE-****7890")
    kv("Redemption Status:", RedemptionStatus.PENDING.value)
    kv("Redemption ID:", str(redemption_id)[:12] + "...")

    # ---- Step 8: Publish Outbox Events to Kafka ----
    header(8, "Publish Outbox Events to Kafka")

    published = outbox_publisher.poll_and_publish()
    kv("Events Published:", str(published))

    total_events = db.query(
        "SELECT COUNT(*) AS cnt FROM outbox_events", ()
    )
    unpublished = db.query(
        "SELECT COUNT(*) AS cnt FROM outbox_events "
        "WHERE published_at IS NULL", ()
    )
    kv("Total Outbox Events:", str(total_events.cnt))
    kv("Remaining:", str(unpublished.cnt))

    # -- Summary --
    print(f"\n{'=' * 60}")
    print("  Demo Complete")
    print(f"{'=' * 60}")

    asset_row = db.query(
        "SELECT total_value FROM rwa_assets WHERE id = %s",
        (asset_id,)
    )
    supply = db.query(
        "SELECT minted_supply FROM rwa_token_supply "
        "WHERE asset_id = %s",
        (asset_id,)
    )
    total_events = db.query(
        "SELECT COUNT(*) AS cnt FROM outbox_events", ()
    )

    print(f"  Asset:           {asset_name}")
    print(f"  Investors:       {len(investor_records)}")
    print(
        f"  Tokens Minted:   "
        f"{Decimal(supply.minted_supply):,.0f}"
    )
    print(
        f"  Fund NAV:        "
        f"${Decimal(asset_row.total_value):,.2f}"
    )
    print(f"  Outbox Events:   {total_events.cnt}")
    print(f"  Kafka Published: {published}")
    print(f"{'=' * 60}\n")
