# Real World Asset (RWA) Tokenization System (Python)

<img width="1536" height="1024" alt="873a6360-a86a-4b83-abc3-90a28c4cd317" src="https://github.com/user-attachments/assets/7bf99454-7d2e-447d-9357-8a3d4066e833" />

> **SANDBOX / EDUCATIONAL USE ONLY — NOT FOR PRODUCTION**
> This codebase is a reference implementation designed for learning, prototyping, and architectural exploration. It is **not audited, not legally reviewed, and must not be used to tokenize, manage, or transfer real assets or investor capital.** See the [Production Warning](#-production-warning) section for full details.

---

## Table of Contents

- [Overview](#-overview)
- [What is RWA Tokenization?](#-what-is-rwa-tokenization)
- [Architecture](#-architecture)
- [Core Services](#-core-services)
- [Key Features & Design Patterns](#-key-features--design-patterns)
- [Database Schema](#-database-schema)
- [State Machines](#-state-machines)
- [Real-World Example: BlackRock BUIDL](#-real-world-example-blackrock-buidl)
- [Running in a Sandbox Environment](#-running-in-a-sandbox-environment)
- [Verifying the System](#-verifying-the-system)
  - [Inspecting the PostgreSQL Database](#inspecting-the-postgresql-database)
  - [Checking Kafka Messages](#checking-kafka-messages)
  - [Viewing Outbox Publisher Logs](#viewing-outbox-publisher-logs)
  - [Viewing All Container Logs](#viewing-all-container-logs)
- [Project Structure](#-project-structure)
- [Production Warning](#-production-warning)
- [License](#-license)

---

## Overview

The **RWA Tokenization System** is a Python-based reference implementation that models the full lifecycle of tokenizing a real-world asset — from initial registration through legal structuring, investor onboarding, token minting, daily yield distribution, redemption, and on-chain/off-chain reconciliation.

The system is modeled closely on how institutional-grade tokenization platforms such as **BlackRock BUIDL**, **Franklin OnChain U.S. Government Money Fund**, and **Ondo Finance** operate in practice. It demonstrates how traditional financial infrastructure (custody, KYC, wire transfers, legal entities) integrates with blockchain technology (ERC-1400 security tokens, smart contract transfer restrictions, on-chain confirmation tracking).

| Component | File | Responsibility |
|---|---|---|
| Core Services | `rwa-tokenization.py` | All service classes — registration through reconciliation |
| Database Schema | `rwa-tokenization.sql` | PostgreSQL schema, indexes, views, and immutability triggers |
| Outbox Publisher | `outbox/outbox-publisher.py` | Async Kafka delivery with retry tracking and dead letter queue |
| Signing Gateway | `signing-gateway/gateway.py` | 2-of-3 MPC threshold signing coordinator |
| MPC Nodes | `mpc/node.py` | Individual MPC signing node (simulated) |
| Containerized Service | `rwa-service/rwa-service.py` | Docker-native version of core services |

---

## What is RWA Tokenization?

**Real World Asset tokenization** is the process of representing ownership of a physical or traditional financial asset as a digital token on a blockchain. Examples include:

- A **US Treasury bill fund** where each token = $1.00 of NAV (BlackRock BUIDL)
- A **commercial real estate property** held in an SPV, with tokens representing equity shares
- A **private equity fund** where tokens represent LP interests
- A **commodity** (gold, oil) with tokens backed by physically-held inventory

Unlike permissionless DeFi tokens, RWA tokens are **regulated securities**. Every holder must pass KYC/AML, every transfer must be between whitelisted wallets, and the legal structure (SPV, Trust, LLC) is what gives the token actual enforceable ownership rights over the underlying asset. Without the legal wrapper, the token is just a receipt — not a legal claim.

---

## Architecture

```
                    ┌────────────────────────────────────────────────────┐
                    │                   RWA LIFECYCLE                    │
                    └────────────────────────────────────────────────────┘

  Off-Chain                                              On-Chain
  ─────────────────────────────────────────────────────────────────────

  1. RWARegistry              Register asset
     (register_asset)  ──────────────────────────────► PostgreSQL
                                                        + Outbox Event

  2. LegalWrapperService      Create SPV / Trust / Fund
     (create_legal_wrapper) ─────────────────────────► Legal entity
                                                        + Token supply init

  3. KYCComplianceService     Sanctions + KYC check
     (onboard_investor) ──── External APIs ──────────► Whitelist wallet
                              (OFAC, Securitize)        + Outbox Event

  4. TokenMintingService      Investor wires fiat
     (mint_tokens) ──────────────────────────────────► Lock supply
                                                        + Signing Queue
                                                              │
                                                              ▼
                                                        ERC-1400 mint()
                                                        on Ethereum

  5. NAVCalculationEngine     Daily yield (T-bill APY)
     (calculate_and_           total_value += yield
      distribute_yield) ──────────────────────────────► Rebase tokens
                                                        + Outbox Event

  6. TokenRedemptionService   Investor returns tokens
     (request_redemption) ───────────────────────────► Burn on-chain
     (settle_redemption)                                Wire fiat back

  7. RWAReconciliationEngine  Daily verification
     (reconcile_asset) ──── Blockchain node ─────────► Mismatch alert
                         └── Custodian API              Halt if broken
```

---

## Core Services

### `RWARegistry`
The entry point for any new tokenized asset. Requires the `ADMIN` role. Validates that the custodian is licensed and approved, writes the asset record and an outbox event atomically, and begins the asset at `PENDING_LEGAL` status. Idempotency is enforced at the registration key level, preventing the same asset from ever being registered twice. Every call generates a `request_id` and records the actor and optional `trace_id` in both the state transition and outbox event payload.

### `LegalWrapperService`
Creates the legal entity (SPV, Delaware Trust, LLC, or regulated Fund) that holds the real-world asset. Requires the `ADMIN` role. This is the most critical layer in RWA infrastructure — it establishes the enforceable legal claim that backs each token. Calculates token economics (`price_per_token = total_value / token_supply`), initializes the token supply tracker, and advances the asset state machine to `PENDING_AUDIT`.

### `KYCComplianceService`
Handles full investor onboarding with a three-phase approach. Requires the `COMPLIANCE` role for `onboard_investor()` and `revoke_whitelist()`. First, it runs an OFAC/EU/UN sanctions screening. Then it calls the KYC provider (e.g., Securitize) for identity and accreditation verification. Both external calls happen **outside** the database transaction to avoid holding row locks during slow API calls. Finally, it atomically writes the compliance record and whitelists the investor's wallet address. Also exposes `can_transfer()` — called before every single token transfer — and `revoke_whitelist()` for expired or exited investors.

### `TokenMintingService`
Triggered when an investor's fiat wire is received. Requires the `ADMIN` role for `mint_tokens()` and the `SYSTEM` role for `confirm_mint()`. Uses pessimistic locking (`SELECT ... FOR UPDATE`) on the token supply row to prevent overselling. Atomically reserves tokens, creates the mint record, and writes an outbox event in a single transaction. The signing queue message is sent outside the transaction. On-chain confirmation is handled by `confirm_mint()`, which settles the ledger with a double-entry accounting entry.

### `NAVCalculationEngine`
Runs daily to calculate and distribute yield from the underlying asset (e.g., T-bill interest). Requires the `SYSTEM` role. Computes `daily_yield = total_value × (annual_rate / 365)`, updates the fund's total value, and emits an outbox event that triggers a token rebase on-chain — the mechanism by which investors receive yield as additional tokens rather than as a price increase.

### `TokenRedemptionService`
The inverse of minting. Requires the `ADMIN` role for `request_redemption()` and the `SYSTEM` role for `settle_redemption()`. Manages the full redemption lifecycle: verifying the investor's compliance status, locking their token balance, publishing a burn transaction to the signing queue, and finally settling by releasing supply counters and triggering a fiat wire transfer. The most operationally complex flow because it coordinates three separate systems — blockchain, compliance, and traditional banking — that must all succeed or roll back together.

### `RWAReconciliationEngine`
Daily audit engine that cross-checks four sources of truth: the internal ledger vs. on-chain token supply, the fund NAV vs. the custodian's reported assets under management, and a scan for tokens held by non-whitelisted wallets. Any discrepancy immediately triggers a critical alert and halts all new minting and redemptions until the mismatch is resolved.

### `RWAOutboxPublisher`
Background poller that delivers `outbox_events` to Kafka. Uses `FOR UPDATE SKIP LOCKED` for safe multi-instance operation. Records delivery by inserting into `outbox_published` (append-only — no `published_at` column on `outbox_events` itself), ensuring exactly-once delivery semantics even across crashes and restarts.

Failed deliveries are tracked in `outbox_publish_attempts`. After 5 consecutive failures, the event is moved to the `outbox_dlq` dead letter queue and a copy is published to a `rwa.dlq.<event_type>` Kafka topic for downstream alerting. Dead-lettered events are excluded from future polling via the updated `v_unpublished_events` view.

Runs as a separate Docker service under a restricted `readonly_user` database role that holds only `SELECT` and `UPDATE` on `outbox_events` (UPDATE is required by PostgreSQL for `FOR UPDATE` row locking — the immutability trigger prevents actual data mutation), `SELECT` on `outbox_published`, `INSERT` on `outbox_published`, and `SELECT`/`INSERT` on both `outbox_publish_attempts` and `outbox_dlq`.

---

## Key Features & Design Patterns

### Role-Based Access Control (RBAC)
Every service method enforces role requirements before executing any business logic. Four roles map to distinct operational responsibilities:

| Role | Permitted Operations |
|---|---|
| `ADMIN` | Register assets, create legal wrappers, mint tokens, request redemptions |
| `COMPLIANCE` | Onboard investors (KYC/AML), revoke whitelists |
| `SYSTEM` | Confirm mints, settle redemptions, calculate NAV/yield |
| `SIGNER` | Reserved for MPC signing operations |

The `require_role()` guard raises `UnauthorizedError` if the caller's role is not in the allowed list for that operation. Each `Actor` carries an `actor_id`, `role`, and `name` that are recorded in every state transition and outbox event for full audit traceability.

### Audit Trail with Request Correlation
Every state transition records three additional fields beyond the state change itself: `request_id` (unique per operation), `trace_id` (shared across a multi-step workflow), and `actor` (the RBAC identity that initiated the action). These same fields are embedded in every outbox event payload, enabling end-to-end correlation from a single API call through the database ledger, Kafka event stream, and downstream consumers.

### Full Asset Lifecycle State Machine
Assets progress through a strict sequence: `PENDING_LEGAL → PENDING_AUDIT → APPROVED → TOKENIZED → REDEEMED`. State guards at each transition prevent operations from running out of order — you cannot mint tokens for an asset that hasn't cleared legal review.

### Permissioned Transfers (ERC-1400 Pattern)
Unlike standard ERC-20 tokens, RWA tokens enforce transfer restrictions at the smart contract level. The `can_transfer()` check gates every token movement — both sender and receiver must appear in `whitelisted_wallets`. This mirrors how ERC-1400 security token standards work in production systems like Securitize and Tokeny.

### KYC Expiry with Mandatory Re-verification
Every investor compliance record carries an `expires_at` timestamp. When KYC expires, `revoke_whitelist()` removes the wallet from the whitelist and blocks all future transfers. This models the annual re-verification requirement imposed by regulated platforms handling securities.

### Idempotency at Every Layer
Each service method checks for an existing `idempotency_key` before any side effect. This covers: asset registration, investor onboarding, token minting, NAV calculations, and redemptions. Re-submitted requests return the original result with no duplicate operations.

### Pessimistic Locking on Token Supply
The `rwa_token_supply` row is locked with `SELECT ... FOR UPDATE` before every mint, preventing two concurrent investments from both seeing sufficient supply and both succeeding — the exact double-mint race condition that has caused real losses on production token platforms.

### Transactional Outbox Pattern (All Services)
Every service writes its outbox event in the **same database transaction** as the business record. This eliminates the dual-write problem across all six services. The `RWAOutboxPublisher` delivers events to Kafka only after they are safely committed to Postgres, so downstream services (investor dashboards, compliance systems, on-chain oracles) never miss an event even if the application crashes.

### Dead Letter Queue with Retry Tracking
Failed Kafka deliveries are not silently dropped. Each failed attempt is recorded in `outbox_publish_attempts` with the error message and timestamp. After 5 consecutive failures, the event is moved to `outbox_dlq` and a copy is published to a `rwa.dlq.<event_type>` Kafka topic for downstream alerting. The `v_unpublished_events` view automatically excludes dead-lettered events, so the publisher only retries events that still have remaining attempts. All DLQ tables are append-only with immutability triggers.

### Atomic Ledger Settlement
Token supply updates (`minted_supply += amount`) and record creation are always co-located in the same transaction. During redemption, the supply release and settlement status update are committed together or not at all — the ledger never shows tokens as both outstanding and redeemed.

### Reconciliation as a Hard Stop
The `RWAReconciliationEngine` treats any discrepancy between internal state and on-chain reality as an emergency, not a warning. Minting and redemptions are halted immediately, ensuring financial integrity is never traded off against operational continuity.

### Decimal Precision for Financial Arithmetic
All token amounts and monetary values use Python's `Decimal` type rather than floats, preventing IEEE 754 rounding errors from corrupting financial calculations — a mandatory requirement in any system handling currency.

---

## Database Schema

### `rwa_assets`
The root record for every tokenized asset.

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `type` | VARCHAR(20) | `real_estate`, `treasury_bond`, `private_equity`, `commodity`, `fund` |
| `name` | VARCHAR(256) | Human-readable asset name |
| `total_value` | DECIMAL(38,18) | Current AUM / asset value (updated daily on yield accrual) |
| `jurisdiction` | VARCHAR(64) | Legal jurisdiction of the asset |
| `custodian` | VARCHAR(256) | Licensed custodian holding the underlying asset |
| `status` | VARCHAR(20) | Asset lifecycle state |
| `idempotency_key` | VARCHAR(256) UNIQUE | Prevents duplicate registration |

### `legal_wrappers`
The legal entity bridging off-chain ownership to on-chain tokens.

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `asset_id` | UUID | Foreign key to `rwa_assets` |
| `structure_type` | VARCHAR(20) | `SPV`, `TRUST`, `LLC`, `FUND` |
| `jurisdiction` | VARCHAR(64) | Legal jurisdiction of the entity |
| `token_supply` | DECIMAL(38,18) | Maximum number of tokens that can be minted |
| `price_per_token` | DECIMAL(38,18) | `total_value / token_supply` at issuance |
| `status` | VARCHAR(20) | Wrapper status |

### `investor_compliance`
KYC/AML record per investor, tied to a specific wallet address.

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `investor_id` | UUID | Investor identifier |
| `wallet_address` | VARCHAR(256) | On-chain Ethereum address |
| `tier` | VARCHAR(20) | `retail`, `accredited`, `qualified`, `institutional` |
| `jurisdiction` | VARCHAR(64) | Investor's legal jurisdiction |
| `status` | VARCHAR(20) | `pending`, `approved`, `rejected`, `expired` |
| `kyc_reference` | VARCHAR(256) | Reference ID from KYC provider (e.g., Securitize) |
| `idempotency_key` | VARCHAR(256) UNIQUE | Prevents duplicate onboarding |
| `expires_at` | TIMESTAMP | When KYC expires — re-verification required |

### `whitelisted_wallets`
The gate for all token transfers. Every row is a wallet address that may legally hold and transfer tokens.

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `investor_id` | UUID | Foreign key to investor |
| `wallet_address` | VARCHAR(256) UNIQUE | On-chain Ethereum address |
| `tier` | VARCHAR(20) | Investor classification |

### `token_mints`
A record for every token issuance event.

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `asset_id` | UUID | Foreign key to `rwa_assets` |
| `investor_id` | UUID | Investor receiving tokens |
| `wallet_address` | VARCHAR(256) | Destination wallet address |
| `token_amount` | DECIMAL(38,18) | Number of tokens issued |
| `fiat_received` | DECIMAL(38,18) | USD amount wired by investor |
| `status` | VARCHAR(20) | Mint lifecycle state |
| `tx_hash` | VARCHAR(256) | On-chain transaction hash |
| `block_number` | BIGINT | Ethereum block number |
| `idempotency_key` | VARCHAR(256) UNIQUE | Prevents duplicate mints |
| `confirmed_at` | TIMESTAMP | NULL = in-flight; NOT NULL = confirmed on-chain |

### `rwa_token_supply`
Tracks minted vs. total supply per asset with pessimistic locking.

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `asset_id` | UUID UNIQUE | Foreign key to `rwa_assets` (one row per asset) |
| `total_supply` | DECIMAL(38,18) | Maximum token supply |
| `minted_supply` | DECIMAL(38,18) | Currently minted tokens (default 0) |

### `nav_calculations`
Daily NAV snapshots and yield calculations.

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `asset_id` | UUID | Foreign key to `rwa_assets` |
| `total_value` | DECIMAL(38,18) | Fund value at time of calculation |
| `daily_yield` | DECIMAL(38,18) | Yield amount for the day |
| `yield_rate` | DECIMAL(38,18) | Annual yield rate used |
| `calculated_at` | TIMESTAMP | When the calculation was performed |
| `idempotency_key` | VARCHAR(256) UNIQUE | One calculation per asset per day |

### `token_redemptions`
Tracks the full redemption lifecycle from request to fiat settlement.

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `asset_id` | UUID | Foreign key to `rwa_assets` |
| `investor_id` | UUID | Investor redeeming tokens |
| `wallet_address` | VARCHAR(256) | Source wallet address |
| `token_amount` | DECIMAL(38,18) | Number of tokens being redeemed |
| `bank_account` | VARCHAR(256) | Fiat destination for wire transfer |
| `status` | VARCHAR(20) | Redemption lifecycle state |
| `idempotency_key` | VARCHAR(256) UNIQUE | Prevents duplicate redemptions |
| `settled_at` | TIMESTAMP | NULL until fiat wire completes |

### `outbox_events`
Reliable event delivery buffer — shared across all services. Append-only; delivery status is tracked in `outbox_published`.

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `aggregate_id` | VARCHAR(256) | ID of the entity that produced the event |
| `event_type` | VARCHAR(256) | e.g. `rwa.registered`, `token.mint_confirmed`, `nav.yield_calculated` |
| `payload` | JSONB | Full event data |
| `created_at` | TIMESTAMP | When the event was written |

### `outbox_published`
Tracks which outbox events have been delivered to Kafka. A row here means the event was successfully published. This replaces a mutable `published_at` column on `outbox_events`, keeping both tables append-only.

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `event_id` | UUID UNIQUE | Foreign key to `outbox_events` |
| `published_at` | TIMESTAMP | When Kafka delivery succeeded |

### `outbox_publish_attempts`
Records each failed Kafka delivery attempt. Used by the outbox publisher to count retries before dead-lettering an event. Append-only.

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `event_id` | UUID | Foreign key to `outbox_events` |
| `error_message` | TEXT | Error from the failed delivery |
| `attempted_at` | TIMESTAMP | When the attempt occurred |

### `outbox_dlq`
Dead letter queue for events that exceeded the maximum retry count (default: 5). Once an event is dead-lettered, it is excluded from `v_unpublished_events` and will not be retried. Append-only.

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `event_id` | UUID UNIQUE | Foreign key to `outbox_events` |
| `error_message` | TEXT | Reason for dead-lettering |
| `attempts` | INTEGER | Total delivery attempts before giving up |
| `created_at` | TIMESTAMP | When the event was dead-lettered |

### `state_transitions`
Append-only audit log of every state change in the system. Each row captures the entity, the transition, and full RBAC context (who did it, which request, which trace).

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `entity_type` | VARCHAR(20) | `asset`, `mint`, `redemption`, `compliance` |
| `entity_id` | UUID | ID of the entity that changed state |
| `from_state` | VARCHAR(20) | Previous state (NULL for initial transitions) |
| `to_state` | VARCHAR(20) | New state |
| `metadata` | JSONB | Additional context (e.g., reason for revocation) |
| `request_id` | UUID | Unique ID for this operation |
| `trace_id` | UUID | Shared ID across a multi-step workflow |
| `actor` | VARCHAR(256) | RBAC identity (`role:name`) that initiated the action |
| `created_at` | TIMESTAMP | When the transition occurred |

### `ledger_entries`
Double-entry accounting journal. Every financial operation (mint, redemption, yield) produces balanced debit/credit pairs. Balances are never stored directly — they are derived from this table via `v_investor_balance`. Append-only.

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `asset_id` | UUID | Foreign key to `rwa_assets` |
| `entry_type` | VARCHAR(20) | `mint`, `redemption`, `yield` |
| `debit_account` | VARCHAR(256) | Account debited |
| `credit_account` | VARCHAR(256) | Account credited |
| `amount` | DECIMAL(38,18) | Transaction amount |
| `reference_id` | UUID | ID of the originating record (mint, redemption, etc.) |
| `created_at` | TIMESTAMP | When the entry was recorded |

---

## State Machines

### Asset Status
```
                      ┌──────────────────┐
  register_asset ────►│  PENDING_LEGAL   │
                      └────────┬─────────┘
                               │  create_legal_wrapper()
                               ▼
                      ┌──────────────────┐
                      │  PENDING_AUDIT   │
                      └────────┬─────────┘
                               │  audit passes
                               ▼
                      ┌──────────────────┐
                      │    APPROVED      │
                      └────────┬─────────┘
                               │  first mint confirmed
                               ▼
                      ┌──────────────────┐
                      │   TOKENIZED      │
                      └────────┬─────────┘
                               │  all tokens redeemed
                               ▼
                      ┌──────────────────┐
                      │    REDEEMED      │
                      └──────────────────┘
```

### Token Mint Status
```
  mint_tokens() ────► PENDING ──► APPROVED ──► SIGNED ──► BROADCAST
                                                                │
                                                    ┌───────────┴──────────┐
                                                    ▼                      ▼
                                               CONFIRMED               FAILED
```

### Redemption Status
```
  request_redemption() ──► PENDING ──► APPROVED ──► BURNING ──► BURNED ──► SETTLED
                                                                              │
                                                                        (fiat wired)
```

---

## Real-World Example: BlackRock BUIDL

The Python demo in `rwa-tokenization.py` is modeled directly on the **BlackRock USD Institutional Digital Liquidity Fund (BUIDL)**, the largest tokenized money market fund with over **$2.9B AUM** as of 2024. The sandbox scenario uses a fictional equivalent called the **Blackstone Treasury Token Fund (BTTF)** with $500M AUM.

| Real-World Attribute | BUIDL (BlackRock) | BTTF (This Demo) |
|---|---|---|
| Underlying asset | US Treasury Bills | US Treasury Bills |
| Legal structure | Delaware Trust | Delaware Trust |
| Custodian | BNY Mellon | BNY Mellon |
| Token price | $1.00 (stable NAV) | $1.00 |
| Total supply | 2.9B tokens | 500M tokens |
| Yield mechanism | Daily token rebase | Daily token rebase |
| Investor type | Institutional only | Institutional only |
| KYC platform | Securitize | Securitize (mocked) |
| Blockchain | Ethereum (ERC-1400) | Ethereum (ERC-1400) |
| Transfer restriction | Whitelisted wallets | Whitelisted wallets |

The Python script mints tokens to three simulated institutional investors:

| Investor | USD Wired | Tokens Minted |
|---|---|---|
| Citadel Securities | $50,000,000 | 50,000,000 BTTF |
| Fidelity Investments | $25,000,000 | 25,000,000 BTTF |
| Goldman Sachs Asset Mgmt | $10,000,000 | 10,000,000 BTTF |
| **Total** | **$85,000,000** | **85,000,000 BTTF** |

---

## Running in a Sandbox Environment

> These instructions are for **local/sandbox use only**. No real assets, KYC data, or investor funds are involved.

### Prerequisites

- Docker and Docker Compose
- Python 3.10+ (for running the script on the host — optional if running only via Docker)

### 1. Clone the Repository

```bash
git clone https://github.com/pavondunbar/RWA-Tokenization-System-Python.git
cd RWA-Tokenization-System-Python
```

### 2. Start the Docker Compose Stack

The stack launches PostgreSQL, Kafka, Zookeeper, the RWA service, the outbox publisher, a signing gateway, and three MPC nodes across three isolated trust-domain networks:

```bash
docker-compose up -d
```

Wait for all containers to reach a healthy/running state:

```bash
docker-compose ps
```

You should see all 9 containers running:

| Container | Role |
|---|---|
| `rwa-ledger-db` | PostgreSQL 16 (append-only ledger) |
| `rwa-kafka` | Kafka broker |
| `rwa-zookeeper` | Zookeeper (Kafka dependency) |
| `rwa-service` | Runs the full demo lifecycle on startup |
| `rwa-outbox-publisher` | Polls outbox events and publishes to Kafka |
| `rwa-signing-gateway` | Bridge between backend and signing domain |
| `rwa-mpc-node-1` | MPC signing node 1 |
| `rwa-mpc-node-2` | MPC signing node 2 |
| `rwa-mpc-node-3` | MPC signing node 3 |

### 3. Run the Demo Script (Host)

If you want to run the demo from the host machine (outside Docker), install dependencies and run:

```bash
pip install psycopg2-binary kafka-python-ng
python3 rwa-tokenization.py
```

The script connects to PostgreSQL on `localhost:5433` and Kafka on `localhost:29092` (the external listener), then runs all eight steps end-to-end — asset registration, legal wrapper creation, investor onboarding, token minting, daily yield calculation, reconciliation, redemption, and outbox publishing.

If Kafka is not reachable from the host, the script falls back to a no-op queue and prints a warning. All database operations still execute normally.

> **Note:** Kafka uses a dual-listener setup — `INTERNAL://kafka:9092` for container-to-container traffic and `EXTERNAL://localhost:29092` for host access. The host-side script connects to the external listener automatically.

### 4. Tear Down and Reset

To stop all containers and destroy all data (volumes):

```bash
docker-compose down -v
```

To restart fresh:

```bash
docker-compose down -v && docker-compose up -d
```

---

## Verifying the System

After running the demo, you can inspect every layer of the system — the PostgreSQL ledger, Kafka event streams, and outbox publisher logs.

### Inspecting the PostgreSQL Database

Connect to the database from the host using `psql` or any SQL client. The database is exposed on port **5433**:

```bash
psql -h localhost -p 5433 -U ledger_user -d rwa
```

Password: `ledger_pass`

Or use `docker exec` to connect from inside the container:

```bash
docker exec -it rwa-ledger-db psql -U ledger_user -d rwa
```

#### Fund Overview

```sql
SELECT a.name, a.total_value, a.custodian,
       lw.structure_type, lw.token_supply, lw.price_per_token
FROM rwa_assets a
JOIN legal_wrappers lw ON lw.asset_id = a.id;
```

#### Investor Compliance Records

```sql
SELECT investor_id, wallet_address, tier, jurisdiction, expires_at
FROM investor_compliance
ORDER BY created_at;
```

#### Whitelisted Wallets (Active)

```sql
SELECT * FROM v_active_whitelists;
```

#### Token Mint Records

```sql
SELECT tm.investor_id, tm.wallet_address, tm.token_amount,
       tm.fiat_received, cs.current_state AS status
FROM token_mints tm
JOIN v_current_state cs
  ON cs.entity_type = 'mint' AND cs.entity_id = tm.id
ORDER BY tm.token_amount DESC;
```

#### State Transition History (with RBAC Audit Trail)

Every state change in the system is recorded as an append-only event with full RBAC context. Query the full audit trail:

```sql
SELECT entity_type, entity_id, from_state, to_state,
       actor, request_id, trace_id, created_at
FROM state_transitions
ORDER BY created_at;
```

Or view only the current state of each entity:

```sql
SELECT * FROM v_current_state ORDER BY entity_type, created_at;
```

Trace all operations from a single workflow using the shared `trace_id`:

```sql
SELECT entity_type, entity_id, from_state, to_state,
       actor, request_id, created_at
FROM state_transitions
WHERE trace_id = '<your-trace-id>'
ORDER BY created_at;
```

#### Double-Entry Ledger

Every financial operation produces balanced debit/credit entries. View the full journal:

```sql
SELECT asset_id, entry_type, debit_account, credit_account,
       amount, reference_id, created_at
FROM ledger_entries
ORDER BY created_at;
```

Verify the ledger balances (debits must equal credits):

```sql
SELECT entry_type,
       SUM(amount) AS total_debits,
       SUM(amount) AS total_credits,
       SUM(amount) - SUM(amount) AS imbalance
FROM ledger_entries
GROUP BY entry_type;
```

#### Investor Balances (Derived from Ledger)

Balances are not stored — they are computed from the append-only ledger entries:

```sql
SELECT * FROM v_investor_balance;
```

#### NAV / Yield Calculations

```sql
SELECT asset_id, total_value, daily_yield, yield_rate, calculated_at
FROM nav_calculations
ORDER BY calculated_at;
```

#### Current Asset Value (Initial + Accumulated Yield)

```sql
SELECT * FROM v_asset_current_value;
```

#### Token Redemptions

```sql
SELECT tr.investor_id, tr.wallet_address, tr.token_amount,
       tr.bank_account, cs.current_state AS status
FROM token_redemptions tr
JOIN v_current_state cs
  ON cs.entity_type = 'redemption' AND cs.entity_id = tr.id;
```

#### Outbox Events

View all events written by the services:

```sql
SELECT id, event_type, aggregate_id, created_at
FROM outbox_events
ORDER BY created_at;
```

Check which events have been published to Kafka:

```sql
SELECT oe.id, oe.event_type, op.published_at
FROM outbox_events oe
JOIN outbox_published op ON op.event_id = oe.id
ORDER BY op.published_at;
```

Check for any unpublished events (should be empty after the outbox publisher runs):

```sql
SELECT * FROM v_unpublished_events;
```

#### Dead Letter Queue

View events that permanently failed delivery after exhausting all retries:

```sql
SELECT dlq.event_id, oe.event_type, dlq.attempts,
       dlq.error_message, dlq.created_at
FROM outbox_dlq dlq
JOIN outbox_events oe ON oe.id = dlq.event_id
ORDER BY dlq.created_at;
```

View individual delivery attempts for a specific event:

```sql
SELECT event_id, error_message, attempted_at
FROM outbox_publish_attempts
WHERE event_id = '<event-id>'
ORDER BY attempted_at;
```

#### Append-Only Verification

Every table is protected by an immutability trigger. You can verify that UPDATE and DELETE are blocked:

```sql
-- This will fail with: "Table rwa_assets is append-only.
-- UPDATE and DELETE are prohibited."
UPDATE rwa_assets SET name = 'test';
```

#### Full Table Counts

Quick overview of how many records each table contains:

```sql
SELECT 'rwa_assets' AS table_name, COUNT(*) FROM rwa_assets
UNION ALL SELECT 'legal_wrappers', COUNT(*) FROM legal_wrappers
UNION ALL SELECT 'investor_compliance', COUNT(*) FROM investor_compliance
UNION ALL SELECT 'whitelisted_wallets', COUNT(*) FROM whitelisted_wallets
UNION ALL SELECT 'token_mints', COUNT(*) FROM token_mints
UNION ALL SELECT 'nav_calculations', COUNT(*) FROM nav_calculations
UNION ALL SELECT 'token_redemptions', COUNT(*) FROM token_redemptions
UNION ALL SELECT 'state_transitions', COUNT(*) FROM state_transitions
UNION ALL SELECT 'ledger_entries', COUNT(*) FROM ledger_entries
UNION ALL SELECT 'outbox_events', COUNT(*) FROM outbox_events
UNION ALL SELECT 'outbox_published', COUNT(*) FROM outbox_published
UNION ALL SELECT 'outbox_publish_attempts', COUNT(*) FROM outbox_publish_attempts
UNION ALL SELECT 'outbox_dlq', COUNT(*) FROM outbox_dlq;
```

### Checking Kafka Messages

#### List All Kafka Topics

The outbox publisher creates topics with the naming pattern `rwa.<event_type>`:

```bash
docker exec rwa-kafka kafka-topics \
  --bootstrap-server localhost:9092 --list
```

Expected topics after a full demo run include:

| Topic | Source |
|---|---|
| `rwa.rwa.registered` | Asset registration |
| `rwa.investor.onboarded` | KYC/AML approval |
| `rwa.token.mint_requested` | Token mint initiated |
| `rwa.token.mint_confirmed` | On-chain mint confirmed |
| `rwa.nav.yield_calculated` | Daily yield distribution |
| `rwa.token.redemption_requested` | Redemption initiated |
| `rwa.token.redemption_settled` | Fiat wire completed |
| `rwa.dlq.<event_type>` | Dead-lettered events (after 5 failed delivery attempts) |

#### Read Messages from a Specific Topic

```bash
docker exec rwa-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic rwa.rwa.registered \
  --from-beginning --timeout-ms 5000
```

#### Read Messages from All Topics

To consume messages from all `rwa.*` topics:

```bash
docker exec rwa-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --whitelist 'rwa\..*' \
  --from-beginning --timeout-ms 5000
```

#### Check Topic Details (Partitions, Offsets)

```bash
docker exec rwa-kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe --topic rwa.rwa.registered
```

### Viewing Outbox Publisher Logs

The outbox publisher logs every event it delivers to Kafka:

```bash
docker logs rwa-outbox-publisher
```

Follow the logs in real time:

```bash
docker logs -f rwa-outbox-publisher
```

Expected output shows each event being picked up and published:

```
OutboxPublisher started — polling for events...
2024-01-01 00:00:00,000 INFO __main__: Found 11 unpublished event(s)
2024-01-01 00:00:00,001 INFO __main__: Published event <uuid> -> rwa.rwa.registered
2024-01-01 00:00:00,002 INFO __main__: Published event <uuid> -> rwa.investor.onboarded
...
```

### Viewing All Container Logs

#### RWA Service (Full Demo Output)

The `rwa-service` container runs the full eight-step demo on startup:

```bash
docker logs rwa-service
```

#### Signing Gateway

```bash
docker logs rwa-signing-gateway
```

#### MPC Nodes

```bash
docker logs rwa-mpc-node-1
docker logs rwa-mpc-node-2
docker logs rwa-mpc-node-3
```

#### All Containers at Once

```bash
docker-compose logs
```

Follow all logs in real time:

```bash
docker-compose logs -f
```

---

## Project Structure

```
RWA-PYTHON/
│
├── docker-compose.yaml            # Full stack: DB, Kafka, services, signing
├── rwa-tokenization.py            # Host-runnable demo (all service classes)
├── rwa-tokenization.sql           # PostgreSQL schema (tables + indexes)
├── pyproject.toml                 # Project metadata and dependencies
├── LICENSE                        # License file
│
├── db/init/
│   ├── 001-schema.sql             # Auto-loaded by Postgres on first boot
│   └── 002-readonly-user.sql      # Restricted user for outbox publisher
│
├── rwa-service/
│   ├── Dockerfile
│   └── rwa-service.py             # Containerized demo (same logic)
│
├── outbox/
│   ├── Dockerfile
│   └── outbox-publisher.py        # Polls outbox_events, publishes to Kafka
│
├── signing-gateway/
│   ├── Dockerfile
│   └── gateway.py                 # 2-of-3 MPC threshold signing coordinator
│
└── mpc/
    ├── Dockerfile
    └── node.py                    # Individual MPC signing node
```

---

## Production Warning

**This project is explicitly NOT suitable for production use.** Tokenizing and managing real-world assets is among the most regulated and legally complex activities in financial services. The following critical components are absent or stubbed:

| Missing Component | Risk if Absent |
|---|---|
| Real legal counsel & entity formation | SPV/Trust/Fund has no legal standing |
| Licensed custodian integration | Underlying assets are not actually held or verified |
| Regulated KYC/AML provider (Securitize, etc.) | No actual identity or accreditation verification |
| Real sanctions screening (OFAC API) | Potential sanctions violations and regulatory exposure |
| Smart contract audit (ERC-1400) | Token contract may have exploitable vulnerabilities |
| Securities law compliance | Token may constitute an unregistered securities offering |
| Hardware Security Module (HSM) for signing | Private keys exposed in software |
| Multi-signature approval workflow | Single point of failure for token issuance |
| Custodian API reconciliation | No way to verify on-chain supply matches real-world assets |
| Authentication & authorization | RBAC roles are enforced but there is no authentication layer (no JWT, no API gateway) |
| Comprehensive test suite | Untested edge cases in financial state machines |
| Regulatory registration | Operating without required licenses (e.g., SEC, FINRA, MAS) |

> Tokenizing real-world assets requires coordination between securities lawyers, licensed custodians, regulated KYC providers, smart contract auditors, and financial regulators. **Do not use this code to register, tokenize, or manage any real asset, investor funds, or regulated securities.**

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).

---

*Built with ♥️ by [Pavon Dunbar](https://linktr.ee/pavondunbar)*
