# Real-Time Ad Billing & Settlement Engine

A distributed billing system that processes ad click/impression events at scale, enforcing budget caps and guaranteeing no double-charging under concurrent load or message replay.

Built to demonstrate: Kafka consumer patterns, gRPC service boundaries, Redis atomic operations, idempotency in financial systems, and distributed systems tradeoffs.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         PRODUCER (load-test/producer.py)        │
│   Generates simulated ad click/impression events at config rate  │
└─────────────────────────┬───────────────────────────────────────┘
                          │ JSON events
                          ▼
                  ┌───────────────┐
                  │  Kafka Broker │  topic: ad-events (6 partitions)
                  │               │  topic: ad-events-dlq (3 partitions)
                  └───────┬───────┘
                          │ consume (at-least-once, manual ACK)
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    BILLING ENGINE (Spring Boot)                  │
│                                                                  │
│  ┌──────────────────┐    ┌──────────────────┐                   │
│  │  AdEventConsumer │───▶│ IdempotencyService│ Redis SETNX      │
│  │  (3 concurrent   │    │ (fast pre-check)  │ TTL 7 days       │
│  │   listeners)     │    └──────────────────┘                   │
│  │                  │                                            │
│  │  On duplicate:   │    ┌──────────────────┐                   │
│  │    ACK + skip    │───▶│  BillingService  │                   │
│  │  On transient    │    └────────┬─────────┘                   │
│  │  failure:        │             │ gRPC                         │
│  │    exponential   │             ▼                              │
│  │    backoff +     │    ┌──────────────────┐                   │
│  │    DLQ routing   │    │ BalanceService   │ gRPC call         │
│  └──────────────────┘    │    Client        │                   │
│                          └────────┬─────────┘                   │
│                                   │ write txn                   │
│                                   ▼                             │
│                          ┌──────────────────┐                   │
│                          │   PostgreSQL     │  transaction_log  │
│                          │  (txn log, UNIQUE│  reconciliation   │
│                          │   on event_id)   │  reports          │
│                          └──────────────────┘                   │
└──────────────────────────────┬──────────────────────────────────┘
                               │ gRPC (protobuf, plaintext)
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│               ACCOUNT BALANCE SERVICE (Spring Boot gRPC)         │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  AccountBalanceGrpcService                               │   │
│  │    CheckBalance   → Redis GET balance:{advertiserId}     │   │
│  │    DeductFunds    → Lua script (atomic check + deduct)   │   │
│  │    SeedBalance    → Redis SET (load test setup)          │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Redis                                                   │   │
│  │    balance:{adv-id}          → Long (micros remaining)   │   │
│  │    deduct:processed:{key}    → 1, TTL 7d (idem. marker)  │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘

                    ┌──────────────────────────────┐
                    │  RECONCILIATION (scripts/)    │
                    │  Runs daily (cron)            │
                    │  Compares PostgreSQL totals   │
                    │  against Redis balance deltas │
                    │  Flags any discrepancy        │
                    └──────────────────────────────┘
```

---

## Design Decisions

### Why a separate Account Balance Service?

The balance service owns one thing: the authoritative balance of each advertiser account. Separating it from the billing engine means:

- **Independent scaling**: Under high load, you can scale billing engine consumers horizontally without touching balance state. The balance service can be replicated separately.
- **Single writer**: All balance mutations go through one service, which makes it easier to reason about correctness and audit who changed what.
- **Clear ownership in an org**: In a real company, "who owns advertiser money" is a separate team/surface from "who processes ad events." This boundary reflects that.
- **Testability**: You can test billing logic by mocking the gRPC client without needing a live Redis.

The tradeoff is a network hop for every event. At 1,000 events/s, this is fine — gRPC on localhost is ~0.1–0.3ms. At 100,000 events/s you'd revisit this (async batching, write-through cache).

### Why gRPC over REST for the internal balance call?

| | gRPC | REST/JSON |
|---|---|---|
| Contract | .proto schema — breaking changes are compile errors | Schema optional, drift possible |
| Serialization | Binary protobuf (~5x smaller than JSON) | Text JSON |
| Latency | ~30% lower on p99 for same payload | Higher due to JSON parse |
| Streaming | Native bidirectional streaming | Awkward |
| Codegen | Stubs generated for both services | Manual client |

For an internal synchronous call that fires on every billing event, the typed contract and lower serialization overhead are worth the added build step.

### Why idempotency matters in billing systems

Kafka provides **at-least-once delivery** — a message can be delivered more than once due to consumer restarts, rebalances, or network retries. Without idempotency, a retry would call `DeductFunds` twice for the same ad click and double-charge the advertiser. This is the kind of bug that:
- Breaks advertiser trust immediately
- Is hard to detect (you need reconciliation to catch it)
- Is very hard to reverse at scale

**How it's implemented here (defense in depth):**

1. **Redis SETNX** (`IdempotencyService`): Before calling the balance service, check if we've seen this `event_id`. If yes, skip. Uses `SET NX PX` — atomic, ~0.1ms.
2. **gRPC-layer idempotency** (`balance.proto`): `DeductFundsRequest` carries an `idempotency_key`. The Lua script atomically checks a `deduct:processed:{key}` key in Redis and no-ops if already set.
3. **PostgreSQL UNIQUE constraint** on `event_id`: The database is the last line of defense if both Redis checks fail (e.g., Redis restart between check and write).

Three independent guards means a bug in one layer doesn't cause a billing error.

### Why dead-letter queue + retry instead of dropping failed events?

In billing, dropping an event means one of two bad outcomes:
- The advertiser was charged but no transaction was recorded (lost money, no audit trail)
- The event was never processed (advertiser underpays, revenue loss)

The DLQ pattern means: if processing fails after `MAX_RETRIES` attempts, the event is preserved on the `ad-events-dlq` topic for human review and offline reprocessing. Nothing is silently lost.

**Retry strategy:** Exponential backoff (500ms → 1s → 2s by default) handles transient failures (gRPC timeout, Redis blip) without hammering a struggling downstream service.

---

## Quick Start

```bash
# 1. Start all services
docker compose up --build

# 2. Wait for health checks to pass (~60s), then seed balances
cd load-test
pip install -r requirements.txt
python seed_balances.py

# 3. Run load test (500 events/s for 60s, 5% duplicates to test idempotency)
python load_test.py --rate 500 --duration 60 --duplicate-pct 5

# 4. Run reconciliation
cd ../scripts
pip install -r requirements.txt
python reconcile.py

# 5. View transaction log
docker exec -it $(docker compose ps -q postgres) \
  psql -U billing -c "SELECT status, COUNT(*), SUM(cost_micros) FROM transaction_log GROUP BY status;"
```

---

## Load Test Results

> **Note:** Run `load_test.py` and paste your actual results here. The table below shows the format — do not use these numbers, replace them with yours.

Results are saved as JSON to `load-test/load-test-results/run_<timestamp>.json` after each run. Use these files as evidence in interviews.

| Metric | Result |
|---|---|
| Target rate | _events/s |
| Actual throughput | _events/s |
| Duration | _s |
| Total processed | _ |
| SUCCESS | _ |
| REJECTED (budget) | _ |
| DLQ (max retries) | _ |
| Duplicate leakage | 0 (idempotency held) |
| Cost p50 | _ micros |
| Cost p95 | _ micros |

**How to run and record:**
```bash
python load_test.py --rate 500 --duration 60 --duplicate-pct 5
# Results saved to load-test/load-test-results/run_YYYYMMDD_HHMMSS.json
```

---

## Running Tests

```bash
# Account Balance Service tests (requires Docker for Testcontainers)
cd account-balance-service
mvn test

# Billing Engine integration tests
cd billing-engine
mvn test
```

Key tests:
- `BalanceServiceTest#deductFunds_idempotency_nodoublecharg` — proves Redis Lua script prevents double deduction
- `BalanceServiceTest#deductFunds_concurrent_noOverspend` — 50 concurrent threads, verifies no overspend
- `AdEventConsumerIdempotencyTest#replay_doesNotDoubleCharge` — end-to-end: replaying event doesn't trigger a second gRPC call
- `AdEventConsumerIdempotencyTest#dbConstraint_catchesDuplicate_ifRedisMisses` — proves Postgres UNIQUE is the last-resort guard

---

## Known Limitations / What I'd Do Differently at Scale

**Single Kafka broker:** The compose setup uses one broker. Production needs a 3+ broker cluster with replication factor ≥ 2 for durability.

**No Kafka exactly-once transactions:** I used at-least-once + idempotency, which is the industry standard approach (Stripe, Adyen, Braintree all do this). True EOS in Kafka adds ~20-30% overhead and significant complexity. For an internal billing system processing 1,000 events/s, the tradeoff favors idempotent at-least-once.

**Redis is not durable by default:** I enabled `appendonly yes` in the compose Redis config, but in production you'd run Redis Cluster with replication + persistent snapshots, or use a Redis-compatible managed service (ElastiCache, Upstash) with automatic failover.

**gRPC is synchronous:** Every Kafka message triggers a blocking gRPC call. At very high throughput (>50k events/s) you'd batch DeductFunds requests or use async gRPC stubs.

**No distributed tracing:** Adding OpenTelemetry trace IDs on each event would let you trace a single ad click from Kafka consumer → gRPC call → Redis → PostgreSQL. Critical for debugging billing discrepancies in production.

**Reconciliation is naive:** It compares totals across all time, not just today's delta. A real reconciliation job would also check for gaps (events that never appeared in either system), not just balance mismatches.

---

## Lessons Learned

**gRPC vs REST tradeoff:** gRPC is genuinely better for internal synchronous service calls where you control both sides. The `.proto` contract acts as a living API spec — when I changed the `DeductFundsResponse` to add an `ALREADY_PROCESSED` status, the compile error in the billing engine told me exactly what needed updating. With REST/JSON, that would have been a runtime bug. The tradeoff is tooling: gRPC requires protoc, and debugging raw binary with `curl` is harder than JSON. For external APIs or mobile clients, REST still wins.

**Exactly-once vs at-least-once:** I initially thought I should use Kafka transactions for exactly-once delivery. After implementing idempotency instead, I understood why the industry uses at-least-once + idempotency: you get the same correctness guarantee with lower latency and far less code. Exactly-once in Kafka requires wrapping your entire consumer logic in a Kafka transaction, which forces you to also make your downstream writes transactional in a coordinated way — complexity that compounds at every layer. Idempotency puts the guarantee at the application layer where you can reason about it independently.

**Redis Lua scripts for atomicity:** The race condition between checking a balance and deducting it (TOCTOU — time of check, time of use) is subtle. A naive implementation with two separate Redis commands (`GET` then `SET`) would be wrong under concurrent load. The Lua script runs as a single atomic unit because Redis is single-threaded at the command execution level. This is the correct tool — not `MULTI/EXEC` transactions (which don't support conditionals) and not distributed locks (which add latency and failure modes).

---

## Project Structure

```
ad-billing-engine/
├── docker-compose.yml
├── README.md
├── account-balance-service/
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/
│       └── main/
│           ├── proto/balance.proto          # gRPC contract
│           ├── java/com/adbilling/balance/
│           │   ├── config/RedisConfig.java
│           │   ├── grpc/AccountBalanceGrpcService.java
│           │   └── service/BalanceService.java
│           └── resources/application.yml
├── billing-engine/
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/
│       └── main/
│           ├── proto/balance.proto          # shared contract
│           └── java/com/adbilling/engine/
│               ├── config/
│               ├── consumer/AdEventConsumer.java
│               ├── grpc/BalanceServiceClient.java
│               ├── model/
│               ├── repository/
│               └── service/
│                   ├── BillingService.java
│                   └── IdempotencyService.java
├── scripts/
│   ├── init.sql                            # PostgreSQL schema
│   └── reconcile.py                        # daily reconciliation job
└── load-test/
    ├── producer.py                         # Kafka event generator
    ├── load_test.py                        # load test harness + results
    ├── seed_balances.py                    # seed Redis balances before test
    └── requirements.txt
```
