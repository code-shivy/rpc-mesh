# rpc-mesh

A latency- and freshness-aware load balancer for Solana JSON-RPC endpoints.

Point your client at rpc-mesh instead of a single RPC provider. It probes every
upstream continuously, routes each request to the fastest node that isn't
lagging behind the cluster, and ejects unhealthy endpoints automatically.

```bash
# Before
RPC_URL=https://api.mainnet-beta.solana.com

# After
RPC_URL=http://localhost:8080
```

No SDK changes. web3.js, Anchor, and solana-go all keep working.

> **Status: work in progress.** Health checking and endpoint selection are
> implemented and tested. The proxy hot path, Prometheus metrics, and
> deployment are in progress — see [Roadmap](#roadmap).

---

## Why not nginx?

Any load balancer can round-robin across upstreams. What a generic one can't
know is that **a Solana RPC node can be fast and wrong.**

RPC nodes drift behind the cluster head. A node that's 500 slots behind still
answers in 20ms — with stale account state. To nginx it looks like the best
endpoint in the pool. To your users it looks like a balance that hasn't
updated, or a transaction that "didn't land."

rpc-mesh tracks each endpoint's current slot, computes its lag against the
highest slot in the pool, and excludes anything beyond a configurable
threshold — *before* considering latency. Freshness gates the candidate set;
speed only breaks the tie.

---

## Health vs. readiness

Two endpoints that look similar and mean very different things. Conflating
them is a common and expensive mistake.

| | Question | Depends on upstreams? | Wrong answer causes |
|---|---|---|---|
| `GET /health` | Is this process alive? | **No** | Restart loops that fix nothing |
| `GET /ready` | Should traffic be sent here? | **Yes** | Traffic sent into a black hole |

If every upstream RPC provider is down, `/ready` returns `503` and the load
balancer stops sending traffic. `/health` still returns `200`, because
restarting rpc-mesh would not bring Solana's RPC providers back — it would
just add churn and lose warm connection pools during an incident.

Kubernetes wires these to `livenessProbe` and `readinessProbe` respectively.
Pointing a liveness probe at a dependency-aware endpoint is how a single
upstream outage turns into a cluster-wide crash loop.

---

## How endpoint selection works

Every `HEALTH_INTERVAL`, all endpoints are probed **in parallel** (one
goroutine each, `WaitGroup`-joined). Each probe issues `getHealth` and
`getSlot` under a per-probe deadline.

Results are applied to the pool in a **single batched write**. This matters:
if results were applied one at a time, `maxSlot` would move mid-update and
slot lag would be computed against a shifting baseline, causing endpoints to
flap in and out of the candidate set.

Selection, in order:

1. Drop unhealthy endpoints.
2. Drop endpoints where `maxSlot - slot > MAX_SLOT_LAG`.
3. Among the rest, pick the lowest EWMA latency. Ties broken randomly.

### Design decisions worth calling out

**EWMA, not raw latency.** A single GC pause on a remote node would evict an
otherwise-good endpoint. Smoothing (α = 0.2) makes routing respond to trends
rather than noise.

**Unprobed ≠ instant.** An endpoint with no latency samples is treated as
`maxDuration`, not `0`. Otherwise every unprobed endpoint would beat every
measured one. At startup all endpoints tie, and the random tie-break spreads
initial load instead of hammering the first one in the list.

**Asymmetric hysteresis.** Three consecutive failures to eject, two
consecutive successes to readmit. Slow to eject so one transient timeout
doesn't remove a good node; slow to readmit so a flapping node doesn't
oscillate. Flapping is worse than a consistently slow endpoint.

**Degrade rather than fail.** If *every* healthy endpoint is lagging,
rpc-mesh serves the request from the best available one and flags it as
degraded rather than returning an error. For most read methods, availability
beats freshness — but the caller is told, so it can be counted and alerted on.

**Dead nodes don't set the watermark.** `maxSlot` is computed only from
endpoints that are healthy and responded successfully. A node that died while
running ahead of the pool would otherwise freeze the watermark permanently
and make every live endpoint look like it was lagging.

**Optimistic start.** Endpoints begin healthy. Starting pessimistic would
mean serving 503s for a full health interval after every deploy.

---

## Concurrency model

The endpoint pool is read by every in-flight request and written by the
background health checker, so it's guarded by a `sync.RWMutex` — reads
massively outnumber writes, and `RLock` doesn't serialize readers against
each other.

The invariant that makes this tractable:

> **Exported fields (`URL`, `Name`, `Weight`) are immutable and lock-free.**
> **Unexported fields (health, slot, latency) are only ever touched inside a
> `Pool` method holding the lock.**

Go's export rules enforce it — code outside the `router` package physically
cannot read mutable state without going through a locking method. `Snapshot()`
returns values rather than pointers for the same reason: handing out
`[]*Endpoint` would let callers read mutable fields with no lock held, a data
race that looks completely innocent at the call site.

One pool-level mutex rather than one per endpoint: per-endpoint locks would
still need a pool-level lock to compute `maxSlot` consistently, which means
two lock levels and a lock-ordering hazard. Known contention point; will
revisit if profiling justifies it.

`go test -race ./...` runs on every commit.

---

## Quick start

```bash
git clone https://github.com/code-shivy/rpc-mesh
cd rpc-mesh

RPC_ENDPOINTS=https://api.mainnet-beta.solana.com,https://solana-rpc.publicnode.com \
  go run .
```

```bash
curl -s localhost:8080/status | jq
curl -i localhost:8080/ready
```

Watch it live:

```bash
watch -n 1 'curl -s localhost:8080/status | jq'
```

Slots should climb steadily, `latency_ms` converges over the first few cycles,
`slot_lag` sits near zero.

### See failure handling work

```bash
RPC_ENDPOINTS=https://api.mainnet-beta.solana.com,https://not-a-real-rpc.invalid go run .
```

The bad endpoint accumulates failures and flips to `healthy: false` on the
third cycle. Traffic never reaches it.

---

## Configuration

All configuration is via environment variables. Invalid config fails at
startup, not at request time.

| Variable | Default | Description |
|---|---|---|
| `RPC_ENDPOINTS` | *(required)* | Comma-separated upstream RPC URLs |
| `PORT` | `8080` | Listen port |
| `HEALTH_INTERVAL` | `5s` | Time between health check cycles |
| `HEALTH_TIMEOUT` | `2s` | Per-probe deadline; must be < `HEALTH_INTERVAL` |
| `UPSTREAM_TIMEOUT` | `15s` | Deadline for proxied requests |
| `MAX_SLOT_LAG` | `50` | Slots behind head before an endpoint is excluded |
| `EWMA_ALPHA` | `0.2` | Latency smoothing factor, in (0, 1] |
| `FAIL_THRESHOLD` | `3` | Consecutive failures before ejection |
| `OK_THRESHOLD` | `2` | Consecutive successes before readmission |
| `MAX_BODY_BYTES` | `5242880` | Max request body size |

Durations use Go syntax: `5s`, `250ms`, `1m30s`.

**API keys in RPC URLs are never logged or exported as metric labels.**
Endpoint names are derived from the hostname only, so a URL like
`https://mainnet.helius-rpc.com/?api-key=SECRET` appears everywhere as
`mainnet.helius-rpc.com`.

`HEALTH_TIMEOUT < HEALTH_INTERVAL` is validated at startup — if a probe can
outlive its cycle, probes pile up and goroutines leak under sustained
upstream slowness.

---

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/health` | Liveness. Always 200 while the process runs. |
| `GET` | `/ready` | Readiness. 503 when no endpoint is usable. |
| `GET` | `/status` | Per-endpoint health, slot, slot lag, EWMA latency. |
| `POST` | `/` | JSON-RPC proxy *(in progress)* |
| `GET` | `/metrics` | Prometheus metrics *(in progress)* |

---

## Development

```bash
go vet ./...
go test -race ./...
```

Tests inject a `ProbeFunc` rather than hitting the network, so slot lag,
timeouts, ejection, and recovery are all covered deterministically with no
external dependencies. Concurrency behavior — parallel fan-out, deadline
enforcement, clean shutdown on context cancellation — is tested directly.

## 📁 Repo Structure

```
rpc-mesh/
├── main.go                       # Entry point: config, pool, health checker, HTTP server
├── go.mod / go.sum
├── router/
│   ├── types.go                  # Endpoint, Pool, selection logic
│   ├── health_check.go           # Parallel probing, hysteresis, EWMA
│   ├── pool_test.go
│   └── health_check_test.go
├── config/
│   └── config.go                 # Env parsing and startup validation
└── README.md
```

---

## Roadmap

- [x] Config loading and validation
- [x] Endpoint pool with slot-lag-aware selection
- [x] Parallel health checker with hysteresis and EWMA
- [x] Graceful shutdown
- [ ] JSON-RPC proxy with method-aware routing and retry
- [ ] Prometheus metrics and Grafana dashboard
- [ ] Benchmark harness with p50/p95/p99 comparison
- [ ] Docker + docker-compose
- [ ] Multi-region deployment