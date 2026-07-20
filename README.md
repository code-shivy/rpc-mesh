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


## Health vs. readiness

Two endpoints that look similar and mean very different things. Conflating 
them is a common and expensive mistake.





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
