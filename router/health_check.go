package router

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gagliardetto/solana-go/rpc"
)

// ProbeFunc performs one health probe against an endpoint and returns the
// observed slot. Injectable so tests can simulate lag and failure without
// standing up fake RPC servers.
type ProbeFunc func(ctx context.Context, ep *Endpoint) (uint64, error)

type HealthChecker struct {
	pool     *Pool
	interval time.Duration
	timeout  time.Duration
	probe    ProbeFunc

	// clients is built once in the constructor and never written after.
	// Read-only maps are safe for concurrent reads with no lock —
	// but note that a map written even once concurrently is a hard
	// runtime throw, not a subtle race. Keep it immutable.
	clients map[string]*rpc.Client
}

func NewHealthChecker(pool *Pool, interval, timeout time.Duration) *HealthChecker {
	hc := &HealthChecker{
		pool:     pool,
		interval: interval,
		timeout:  timeout,
		clients:  make(map[string]*rpc.Client),
	}

	// One client per endpoint, created once. Each rpc.Client owns an HTTP
	// client with its own connection pool — creating one per probe would
	// mean a fresh TCP + TLS handshake every cycle, which is exactly the
	// latency we're supposed to be eliminating.
	for _, ep := range pool.Endpoints() {
		hc.clients[ep.URL] = rpc.New(ep.URL)
	}

	hc.probe = hc.defaultProbe
	return hc
}

// Run blocks until ctx is cancelled, probing every interval.
// Intended to be called as `go hc.Run(ctx)`.
func (hc *HealthChecker) Run(ctx context.Context) {
	// Probe immediately rather than waiting a full interval — otherwise
	// the first `interval` of traffic routes on zero information.
	hc.RunCycle(ctx)

	ticker := time.NewTicker(hc.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("health checker: stopping")
			return
		case <-ticker.C:
			hc.RunCycle(ctx)
		}
	}
}

// RunCycle probes every endpoint in parallel and applies all results in a
// single batched write.
//
// Parallel, not sequential: 5 endpoints at 200ms each is 1s sequentially,
// and by the time the last one reports, the first one's slot reading is a
// cycle stale. Fan out and the cycle costs one probe's latency.
func (hc *HealthChecker) RunCycle(ctx context.Context) {
	endpoints := hc.pool.Endpoints()
	if len(endpoints) == 0 {
		return
	}

	// Pre-allocated: each goroutine writes exactly one index it alone owns,
	// so no mutex is needed. `append` here would be a genuine data race —
	// it mutates the shared slice header and may reallocate.
	results := make([]ProbeResult, len(endpoints))

	var wg sync.WaitGroup
	for i, ep := range endpoints {
		wg.Add(1) // before `go`, never inside — otherwise Wait can win the race
		go func() {
			defer wg.Done()

			// Per-probe deadline. Without it, one hung endpoint holds a
			// goroutine and a WaitGroup slot forever and the whole cycle
			// never completes.
			probeCtx, cancel := context.WithTimeout(ctx, hc.timeout)
			defer cancel()

			start := time.Now()
			slot, err := hc.probe(probeCtx, ep)
			elapsed := time.Since(start)

			results[i] = ProbeResult{
				Endpoint: ep,
				Slot:     slot,
				Latency:  elapsed,
				Err:      err,
			}
		}()
	}

	// Wait returning establishes happens-before: every write above is
	// visible here. Safe to read all of results without synchronization.
	wg.Wait()

	// If we were cancelled mid-cycle, the results are all spurious
	// timeouts. Applying them would eject every endpoint on shutdown.
	if ctx.Err() != nil {
		return
	}

	hc.pool.ApplyProbes(results)
}

// defaultProbe issues getHealth then getSlot against a real endpoint.
func (hc *HealthChecker) defaultProbe(ctx context.Context, ep *Endpoint) (uint64, error) {
	client, ok := hc.clients[ep.URL]
	if !ok {
		return 0, fmt.Errorf("no client for endpoint %s", ep.Name)
	}

	// getHealth is the node's own verdict: "ok" means it considers itself
	// caught up with the cluster. It's cheap and catches nodes that are
	// serving but knowingly behind.
	if _, err := client.GetHealth(ctx); err != nil {
		return 0, fmt.Errorf("getHealth: %w", err)
	}

	// CommitmentProcessed gives the node's most recent slot, including
	// blocks not yet confirmed. That's what we want here: we're measuring
	// how far behind this node is, not reading data users will act on.
	// Confirmed/finalized would add a fixed lag to every endpoint equally
	// and mask real drift.
	slot, err := client.GetSlot(ctx, rpc.CommitmentProcessed)
	if err != nil {
		return 0, fmt.Errorf("getSlot: %w", err)
	}

	return slot, nil
}