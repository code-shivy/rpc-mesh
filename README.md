# rpc-mesh

A latency- and freshness-aware load balancer for Solana JSON-RPC endpoints.

Point your client at rpc-mesh instead of a single RPC provider. It probes every
upstream continuously, routes each request to a node that isn't lagging behind
the cluster, and ejects unhealthy endpoints automatically.

```bash
# Before
RPC_URL=https://api.mainnet-beta.solana.com

# After
RPC_URL=http://localhost:8080
```

No SDK changes. web3.js, Anchor, and solana-go all keep working — the official
Solana CLI does too:

```bash
solana --url http://localhost:8080 epoch-info
```

> **Status: work in progress.** Config, health checking, endpoint selection, the
> JSON-RPC proxy, and Prometheus metrics are implemented and tested. Benchmark
> numbers, Docker packaging, and deployment are in progress — see
> [Roadmap](#roadmap).

---

## Why not nginx?

Any load balancer can round-robin across upstreams. What a generic one can't
know is that **a Solana RPC node can be fast and wrong.**

RPC nodes drift behind the cluster head. A node that's 500 slots behind still
answers in 20ms — with stale account state. To nginx it looks like the best
endpoint in the pool. To your users it looks like a balance that hasn't
updated, or a transaction that "didn't land."

rpc-mesh tracks each endpoint's current slot, computes its lag against the
highest slot observed across the pool, and excludes anything beyond a
configurable threshold — *before* considering latency. Freshness gates the
candidate set; speed only ranks what's left.

Slot drift is not hypothetical. Two healthy public endpoints, probed
simultaneously, routinely disagree about the current head of the chain:

```json
{"name": "api.mainnet-beta.solana.com", "slot": 434874758, "slot_lag": 0}
{"name": "solana-rpc.publicnode.com",   "slot": 434874757, "slot_lag": 1}
```

One slot is roughly 400ms of drift — noise at that magnitude, but the mechanism
is identical when an endpoint falls 200 slots behind under load.

### The public endpoint landscape

Four free Solana RPC endpoints, four distinct failure modes, none discoverable
without trying:

| Endpoint | Result |
|---|---|
| `solana.api.onfinality.io/public` | Rate limited immediately (`-32029`) |
| `rpc.ankr.com/solana` | API key rejected (`-32052`, HTTP 403) |
| `solana.drpc.org` | Chain not on free plan (`code 35` — not a standard JSON-RPC code) |
| `endpoints.omniatech.io/v1/sol/mainnet/public` | Cloudflare 521, origin down |

Three of four dead. This is the case for health checking, stated more
convincingly than any architecture diagram: you cannot hardcode one provider and
expect it to keep working, and you cannot tell which are alive without probing.

---

## Health vs. readiness

Two endpoints that look similar and mean very different things. Conflating them
is a common and expensive mistake.

| | Question | Depends on upstreams? | Wrong answer causes |
|---|---|---|---|
| `GET /health` | Is this process alive? | **No** | Restart loops that fix nothing |
| `GET /ready` | Should traffic be sent here? | **Yes** | Traffic sent into a black hole |

If every upstream RPC provider is down, `/ready` returns `503` and the load
balancer stops sending traffic. `/health` still returns `200`, because
restarting rpc-mesh would not bring Solana's RPC providers back — it would just
add churn and lose warm connection pools during an incident.

Kubernetes wires these to `livenessProbe` and `readinessProbe` respectively.
Pointing a liveness probe at a dependency-aware endpoint is how a single
upstream outage turns into a cluster-wide crash loop.

---

## How endpoint selection works

Every `HEALTH_INTERVAL`, all endpoints are probed **in parallel** (one goroutine
each, `WaitGroup`-joined). Each probe issues `getHealth` then `getSlot` under a
per-probe deadline.

Results are applied to the pool in a **single batched write**. This matters: if
results were applied one at a time, `maxSlot` would move mid-update and slot lag
would be computed against a shifting baseline, causing endpoints to flap in and
out of the candidate set.

Selection, in order:

1. Drop unhealthy endpoints and the endpoint being retried away from.
2. Partition by freshness — anything with `maxSlot - slot > MAX_SLOT_LAG` is set
   aside.
3. Among fresh candidates, sample two at random and route to the faster
   (power-of-two-choices), treating latencies within `nearTieRatio` as tied.
4. If nothing is fresh, serve from the best lagging endpoint and flag the
   response as degraded.

### Design decisions worth calling out

**Power-of-two-choices, not lowest-wins.** Strict "route to the fastest" is
winner-take-all: one endpoint absorbs 100% of traffic until its measured latency
rises above the runner-up's. On a pool of free-tier providers that means hitting
one endpoint's rate limit while the others idle — and it leaves the unused
endpoints' latency data low-resolution, since only health probes ever refresh
it. P2C keeps tail latency close to always-pick-the-fastest while spreading
load. Note it only does useful work at three or more candidates; with two, both
are always sampled and the comparison is unconditional.

**Near-equal latencies count as tied.** Probe RTT is noisy — two sequential RPC
calls, smoothed but not precise. A 20% measured gap that is really jitter should
not send 100% of traffic one way. Within `nearTieRatio` (1.25) the choice is
random; beyond it the gap is large enough to act on.

**EWMA, not raw latency.** A single GC pause on a remote node would evict an
otherwise-good endpoint. Smoothing (α = 0.2) makes routing respond to trends
rather than spikes. A live example — one slow sample followed by five cycles of
decay back to baseline:

```
337ms → 450ms → 426ms → 407ms → 391ms → 379ms → 369ms
```

**Unprobed ≠ instant.** An endpoint with no latency samples is treated as
`maxDuration`, not `0`. Otherwise every unprobed endpoint would beat every
measured one. At startup all endpoints tie, and the random tie-break spreads
initial load instead of hammering the first one in the list.

**Asymmetric hysteresis.** Three consecutive failures to eject, two consecutive
successes to readmit. Slow to eject so one transient timeout doesn't remove a
good node; slow to readmit so a flapping node doesn't oscillate. Flapping is
worse than a consistently slow endpoint.

**Degrade rather than fail.** If *every* healthy endpoint is lagging, rpc-mesh
serves from the best available one and flags it rather than returning an error.
For most read methods availability beats freshness — but the caller is told, via
`X-RPC-Mesh-Degraded` and a counter, so it can be alerted on.

**Dead nodes don't set the watermark.** `maxSlot` is computed only from
endpoints that are healthy and responded successfully. A node that died while
running ahead of the pool would otherwise freeze the watermark permanently and
make every live endpoint look like it was lagging.

**Never-probed endpoints report zero lag.** An endpoint that has never reported
a slot would otherwise show a lag of the entire chain height (~434,000,000).
That is not a lag, it is the absence of a measurement — and as a Prometheus
series it destroys the y-axis of any dashboard graphing lag across endpoints.
The right signal for a dead endpoint is `rpcmesh_endpoint_healthy`.

**Optimistic start.** Endpoints begin healthy. Starting pessimistic would mean
serving 503s for a full health interval after every deploy.

---

## Proxy behavior

**Writes are never retried.** A timeout on `sendTransaction` is ambiguous — the
transaction may already be in a leader's queue. Retrying risks a double-send,
which for a non-idempotent program means real financial loss. Reads retry once
on a different endpoint; `sendTransaction`, `requestAirdrop`, and batches (which
may contain a write) fail fast and let the client, which knows the signature and
can poll for it, decide.

**The request body is peeked, not parsed.** Routing needs the `method` field and
nothing else. `encoding/json` decodes into a two-field struct and ignores the
rest, so a 2MB request costs no more than a small one — and nothing breaks when
Solana adds a field.

**Connection reuse is most of the latency win.** Go's default transport keeps
two idle connections per host. Under any concurrency you exceed that constantly
and pay a fresh TCP and TLS handshake — 100–200ms to a distant node, on a
request whose real work is 20ms. `MaxIdleConnsPerHost` is set to 100.

**JSON-RPC errors inside HTTP 200 are counted.** JSON-RPC signals application
failures in the body, so status code alone tells you nothing:

```
$ curl -X POST localhost:8080 -d '{"jsonrpc":"2.0","id":1,"method":"garbage123"}'
HTTP/1.1 200 OK
{"jsonrpc":"2.0","error":{"code":-32601,"message":"Method not found"},"id":1}
```

Without inspecting the body, a dashboard would show 100% success while every
client call fails. rpc-mesh buffers the first 4KB of each response, extracts any
error code into `rpcmesh_rpc_errors_total`, and streams the rest untouched.
Larger responses skip parsing entirely — a 30MB `getProgramAccounts` result is a
success, not an error object.

**Upstream-scoped headers are stripped.** `Alt-Svc` would advertise HTTP/3 on a
port rpc-mesh doesn't serve. Upstream CORS headers describe the upstream's
policy — one public endpoint returns a literal `backend_traffic` as the allowed
origin, which browsers reject. rpc-mesh sets its own.

---

## Concurrency model

The endpoint pool is read by every in-flight request and written by the
background health checker, so it's guarded by a `sync.RWMutex` — reads massively
outnumber writes, and `RLock` doesn't serialize readers against each other.

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
still need a pool-level lock to compute `maxSlot` consistently, which means two
lock levels and a lock-ordering hazard. Known contention point; will revisit if
profiling justifies it.

`go test -race ./...` runs on every commit.

---

## Quick start

```bash
git clone https://github.com/code-shivy/rpc-mesh
cd rpc-mesh

RPC_ENDPOINTS="https://api.mainnet-beta.solana.com,https://solana-rpc.publicnode.com" \
  go run .
```

```bash
curl -s localhost:8080/status | jq
curl -i localhost:8080/ready
```

Watch routing state live:

```bash
watch -n 1 'curl -s localhost:8080/status | jq'
```

Slots climb steadily, `latency_ms` converges over the first few cycles,
`slot_lag` sits near zero.

### See failure handling work

```bash
RPC_ENDPOINTS="https://api.mainnet-beta.solana.com,https://solana.drpc.org" go run .
```

`solana.drpc.org` is a real endpoint that rejects free-tier Solana traffic. It
accumulates failures, flips to `healthy: false` on the third cycle, and receives
no traffic thereafter:

```json
{
  "name": "solana.drpc.org",
  "healthy": false,
  "slot": 0,
  "last_error": "getHealth: rpc error 35: chain is not available on free plan"
}
```

---

## Benchmarking

```bash
# terminal 1
RPC_ENDPOINTS="https://api.mainnet-beta.solana.com,https://solana-rpc.publicnode.com" go run .

# terminal 2 — wait ~30s for EWMAs to converge first
go run ./cmd/bench \
  -endpoints "https://api.mainnet-beta.solana.com,https://solana-rpc.publicnode.com" \
  -n 300 -c 10
```

`cmd/bench` measures each upstream individually and then rpc-mesh, through
identically configured HTTP transports, and reports p50/p95/p99 for each.

**rpc-mesh cannot beat the fastest endpoint** — it adds a network hop, so that
comparison should come out slightly negative. Any benchmark showing otherwise
has a bug, most likely mismatched transport settings between the baseline and
the proxy.

The meaningful comparison is against a *randomly chosen* endpoint, which is what
a developer gets by hardcoding one provider without knowing which is fastest.
The other two wins are tail latency — routing away from an endpoint having a bad
minute within one health cycle — and availability, where an endpoint returning
errors for 100% of requests contributes 0% through the mesh.

Load is closed-loop (each worker waits for a response before issuing the next),
which understates tail latency under load — "coordinated omission." Every target
is measured identically, so the comparison holds even though absolute numbers
are optimistic.

*Numbers to be published once the harness has run against a stable pool.*

---

## Configuration

All configuration is via environment variables. Invalid config fails at startup,
not at request time.

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
| `CORS_ALLOW_ORIGIN` | `*` | Allowed browser origin. Narrow this when fronting an API-keyed upstream. |

Durations use Go syntax: `5s`, `250ms`, `1m30s`.

**API keys in RPC URLs are never logged or exported as metric labels.** Endpoint
names are derived from the hostname only, so
`https://mainnet.helius-rpc.com/?api-key=SECRET` appears everywhere as
`mainnet.helius-rpc.com`.

`HEALTH_TIMEOUT < HEALTH_INTERVAL` is validated at startup — if a probe can
outlive its cycle, probes pile up and goroutines leak under sustained upstream
slowness.

---

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/health` | Liveness. Always 200 while the process runs. |
| `GET` | `/ready` | Readiness. 503 when no endpoint is usable. |
| `GET` | `/status` | Per-endpoint health, slot, slot lag, EWMA latency. |
| `GET` | `/metrics` | Prometheus metrics. |
| `POST` | `/` | JSON-RPC proxy. Forwards to the selected endpoint. |
| `OPTIONS` | `/` | CORS preflight. |

Response headers on proxied requests:

| Header | Meaning |
|---|---|
| `X-RPC-Mesh-Endpoint` | Which upstream served this request |
| `X-RPC-Mesh-Degraded` | Present when every candidate was slot-lagging |
| `X-RPC-Mesh-RPC-Error` | Present when the 2xx body carries a JSON-RPC error |

---

## Metrics

| Metric | Type | Notes |
|---|---|---|
| `rpcmesh_requests_total` | counter | By endpoint, method, status class |
| `rpcmesh_request_duration_seconds` | histogram | Buckets tuned for 5ms–10s, not Go's defaults |
| `rpcmesh_upstream_errors_total` | counter | Transport/HTTP failures by reason |
| `rpcmesh_rpc_errors_total` | counter | JSON-RPC errors inside 2xx responses |
| `rpcmesh_degraded_requests_total` | counter | Served from a lagging endpoint |
| `rpcmesh_retries_total` | counter | Reads retried on another endpoint |
| `rpcmesh_endpoint_healthy` | gauge | 1 if routable |
| `rpcmesh_endpoint_slot` | gauge | Last observed slot |
| `rpcmesh_endpoint_slot_lag` | gauge | Slots behind the pool's head |
| `rpcmesh_endpoint_probe_rtt_seconds` | gauge | EWMA probe RTT (two sequential RPC calls — not comparable to request latency) |
| `rpcmesh_endpoint_last_check_age_seconds` | gauge | Rising steadily means the checker is stuck |
| `rpcmesh_pool_max_slot` | gauge | Reference point for slot lag |

**Method labels are allowlisted.** The method name comes from the request body,
which is attacker-controlled — without an allowlist, a loop posting
`{"method":"random-string-N"}` mints a new time series per request until the
TSDB falls over. Unrecognized methods collapse to `other`. The same applies to
JSON-RPC error codes, since providers invent their own (drpc's `code 35` is not
in any spec).

**Endpoint gauges are computed at scrape time** via a custom `prometheus.Collector`
reading `pool.Snapshot()`, rather than pushed by the health checker. There's no
second copy of the state to keep in sync, and the values are current by
construction.

---

## Repo structure

```
rpc-mesh/
├── main.go                       # Entry point: config, pool, health checker, metrics, HTTP server
├── go.mod / go.sum
├── router/
│   ├── types.go                  # Endpoint, Pool, selection logic
│   ├── health_check.go           # Parallel probing, hysteresis, EWMA
│   ├── proxy.go                  # JSON-RPC forwarding, retry policy, CORS
│   ├── pool_test.go
│   ├── health_check_test.go
│   └── proxy_test.go
├── metrics/
│   ├── prometheus.go             # Counters, histograms, cardinality guards
│   └── pool_collector.go         # Scrape-time gauges from pool state
├── config/
│   └── config.go                 # Env parsing and startup validation
├── cmd/
│   └── bench/
│       └── main.go               # p50/p95/p99 harness: each endpoint vs. the mesh
└── README.md
```

Every directory containing `package main` builds its own binary — `go run .` for
the server, `go run ./cmd/bench` for the harness.

Tests live beside the code they test, in the same package. This is deliberate:
`package router` gives tests access to unexported fields, so they can construct
exact pool states — a specific endpoint 500 slots behind, another two failures
from ejection — without exporting mutable health state just for testing.

Planned, not yet present: `Dockerfile`, `docker-compose.yml`,
`deploy/prometheus.yml`, `deploy/grafana/`.

---

## Development

```bash
go vet ./...
go test -race ./...
```

Tests inject a `ProbeFunc` rather than hitting the network, so slot lag,
timeouts, ejection, and recovery are covered deterministically with no external
dependencies. Concurrency behavior — parallel fan-out, deadline enforcement,
clean shutdown on context cancellation — is tested directly, and the race
detector is the correctness proof for the shared pool.

---

## Roadmap

- [x] Config loading and validation
- [x] Endpoint pool with slot-lag-aware selection
- [x] Parallel health checker with hysteresis and EWMA
- [x] Graceful shutdown
- [x] JSON-RPC proxy with retry-on-read, no-retry-on-write
- [x] Power-of-two-choices routing with a near-tie band
- [x] Prometheus metrics with bounded label cardinality
- [x] Benchmark harness
- [ ] Published benchmark numbers
- [ ] Grafana dashboard, provisioned
- [ ] Docker + docker-compose
- [ ] Quota-aware routing — providers return `X-Ratelimit-*` headers on every response; routing away from an endpoint before it starts refusing you is free information currently thrown away
- [ ] Method-aware routing (expensive calls to a paid tier, cheap reads to free ones)
- [ ] Multi-region deployment
