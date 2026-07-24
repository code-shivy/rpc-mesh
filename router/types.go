package router

import (
	"errors"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/code-shivy/rpc-mesh/config"
)

var (
	// ErrNoEndpoints means every endpoint is unhealthy. Callers must
	// surface this as a 503 — there is nothing sane to fall back to.
	ErrNoEndpoints = errors.New("router: no healthy endpoints available")
)

// Endpoint represents one upstream Solana RPC node.
//
// Exported fields are immutable after construction and safe to read
// without a lock. Unexported fields are mutable state guarded by
// Pool.mu — never touch them outside a Pool method.
type Endpoint struct {
	URL    string
	Name   string
	Weight int

	healthy     bool
	slot        uint64
	ewmaLatency time.Duration
	consecFails int
	consecOKs   int
	lastChecked time.Time
	lastErr     error
}

// EndpointState is an immutable snapshot, safe to hand to metrics
// exporters or HTTP handlers with no lock held.
type EndpointState struct {
	Name        string        `json:"name"`
	URL         string        `json:"url"`
	Healthy     bool          `json:"healthy"`
	Slot        uint64        `json:"slot"`
	SlotLag     uint64        `json:"slot_lag"`
	EWMALatency time.Duration `json:"-"`
	LatencyMS   float64       `json:"latency_ms"`
	LastChecked time.Time     `json:"last_checked"`
	LastError   string        `json:"last_error,omitempty"`
}

// ProbeResult is one health-check observation, produced by the health
// checker and applied to the pool in a single batched write.
type ProbeResult struct {
	Endpoint *Endpoint
	Slot     uint64
	Latency  time.Duration
	Err      error
}

// Pool is the set of upstream endpoints plus their live health state.
//
// One mutex guards the whole pool rather than one per endpoint. Per-endpoint
// locks would still require a pool-level lock to compute maxSlot consistently,
// which means two lock levels and a lock-ordering hazard. Known contention
// point; revisit if pprof shows it matters.
type Pool struct {
	mu        sync.RWMutex
	endpoints []*Endpoint
	maxSlot   uint64

	maxSlotLag uint64
	ewmaAlpha  float64
	failThresh int
	okThresh   int
}

func NewPool(cfg config.Config) (*Pool, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, errors.New("router: pool requires at least one endpoint")
	}

	eps := make([]*Endpoint, 0, len(cfg.Endpoints))
	for _, ec := range cfg.Endpoints {
		eps = append(eps, &Endpoint{
			URL:    ec.URL,
			Name:   ec.Name,
			Weight: ec.Weight,
			// Optimistic start: assume healthy until proven otherwise.
			// Starting pessimistic means serving 503s for a full health
			// interval on every deploy.
			healthy: true,
		})
	}

	return &Pool{
		endpoints:  eps,
		maxSlotLag: cfg.MaxSlotLag,
		ewmaAlpha:  cfg.EWMAAlpha,
		failThresh: cfg.FailThreshold,
		okThresh:   cfg.OKThreshold,
	}, nil
}

// Endpoints returns a copy of the endpoint slice for the health checker to
// iterate. The slice is a copy; the *Endpoint values are shared. Callers may
// read exported fields only.
func (p *Pool) Endpoints() []*Endpoint {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]*Endpoint, len(p.endpoints))
	copy(out, p.endpoints)
	return out
}

// Select picks the best endpoint for a given JSON-RPC method.
func (p *Pool) Select(method string) (*Endpoint, bool, error) {
	return p.SelectExcluding(method, nil)
}

// SelectExcluding picks the best endpoint, skipping `exclude` (used on retry).
//
// Returns (endpoint, degraded, error). degraded == true means no endpoint was
// slot-current and we fell back to a healthy-but-lagging one — the caller
// should log/count this, because it means clients may see stale state.
//
// `method` is unused in v1. It's the seam for sendTransaction routing on Day 4;
// taking it now avoids touching every call site later.
func (p *Pool) SelectExcluding(method string, exclude *Endpoint) (*Endpoint, bool, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var current, lagging []*Endpoint

	for _, ep := range p.endpoints {
		if ep == exclude || !ep.healthy {
			continue
		}
		if p.slotLagLocked(ep) <= p.maxSlotLag {
			current = append(current, ep)
		} else {
			lagging = append(lagging, ep)
		}
	}

	if len(current) > 0 {
		return pickFastest(current), false, nil
	}
	// Availability beats freshness for most read methods. Serve it, but
	// tell the caller so it can be counted and alerted on.
	if len(lagging) > 0 {
		return pickFastest(lagging), true, nil
	}
	return nil, false, ErrNoEndpoints
}

// pickFastest returns the lowest-EWMA candidate, breaking ties randomly.
//
// An unprobed endpoint (ewma == 0) is treated as maxDuration rather than zero.
// Zero would make every unprobed endpoint beat every measured one. At startup
// all endpoints are unprobed, so all tie at maxDuration and the random
// tie-break spreads initial load instead of hammering endpoints[0].
//
// Caller must hold at least a read lock.
func pickFastest(candidates []*Endpoint) *Endpoint {
	best := time.Duration(math.MaxInt64)
	for _, ep := range candidates {
		if l := effectiveLatency(ep); l < best {
			best = l
		}
	}

	tied := candidates[:0:0] // new backing array; don't alias the caller's slice
	for _, ep := range candidates {
		if effectiveLatency(ep) == best {
			tied = append(tied, ep)
		}
	}

	if len(tied) == 1 {
		return tied[0]
	}
	return tied[rand.IntN(len(tied))]
}

func effectiveLatency(ep *Endpoint) time.Duration {
	if ep.ewmaLatency == 0 {
		return time.Duration(math.MaxInt64)
	}
	return ep.ewmaLatency
}

// slotLagLocked returns how far behind the cluster head this endpoint is.
// Guards against underflow: an endpoint can briefly report a slot higher
// than the maxSlot computed on the previous cycle.
func (p *Pool) slotLagLocked(ep *Endpoint) uint64 {
	if ep.slot >= p.maxSlot {
		return 0
	}
	return p.maxSlot - ep.slot
}

// ApplyProbes applies a full cycle of probe results under a single write lock
// and recomputes maxSlot. Batching matters: if results trickled in one lock at
// a time, slot lag would be computed against a moving maxSlot and endpoints
// would flap in and out of the candidate set.
func (p *Pool) ApplyProbes(results []ProbeResult) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, r := range results {
		p.recordProbeLocked(r)
	}
	p.recomputeMaxSlotLocked()
}

// RecordProbe applies a single probe result. Convenience for tests and for
// callers outside the batch path.
func (p *Pool) RecordProbe(r ProbeResult) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.recordProbeLocked(r)
	p.recomputeMaxSlotLocked()
}

func (p *Pool) recordProbeLocked(r ProbeResult) {
	ep := r.Endpoint
	if ep == nil {
		return
	}
	ep.lastChecked = time.Now()

	if r.Err != nil {
		ep.lastErr = r.Err
		ep.consecOKs = 0
		ep.consecFails++
		// Hysteresis: N consecutive failures to eject. Without it, one
		// transient timeout ejects an endpoint and the pool flaps.
		if ep.consecFails >= p.failThresh {
			ep.healthy = false
		}
		// Deliberately do NOT update slot or ewmaLatency on failure —
		// a failed probe carries no latency signal, and keeping the last
		// known slot lets us see how stale the endpoint has become.
		return
	}

	ep.lastErr = nil
	ep.consecFails = 0
	ep.consecOKs++
	if !ep.healthy && ep.consecOKs >= p.okThresh {
		ep.healthy = true
	}

	ep.slot = r.Slot

	// EWMA seeding: on the first sample the old value is zero, and
	// alpha*sample + (1-alpha)*0 badly underestimates. Seed directly.
	if ep.ewmaLatency == 0 {
		ep.ewmaLatency = r.Latency
	} else {
		a := p.ewmaAlpha
		ep.ewmaLatency = time.Duration(a*float64(r.Latency) + (1-a)*float64(ep.ewmaLatency))
	}
}

// recomputeMaxSlotLocked takes the max slot across endpoints that are healthy
// and reported successfully. Excluding failed endpoints prevents a node that
// died while far ahead from permanently inflating maxSlot and making every
// live endpoint look lagging.
func (p *Pool) recomputeMaxSlotLocked() {
	var max uint64
	for _, ep := range p.endpoints {
		if !ep.healthy || ep.lastErr != nil {
			continue
		}
		if ep.slot > max {
			max = ep.slot
		}
	}
	if max > 0 {
		p.maxSlot = max
	}
}

// Snapshot returns immutable copies of every endpoint's state.
//
// Returns values, not pointers. Returning []*Endpoint would let the caller
// read mutable fields with no lock held — a data race that looks completely
// innocent at the call site.
func (p *Pool) Snapshot() []EndpointState {
	p.mu.RLock()
	defer p.mu.RUnlock()

	out := make([]EndpointState, 0, len(p.endpoints))
	for _, ep := range p.endpoints {
		st := EndpointState{
			Name:        ep.Name,
			URL:         ep.URL,
			Healthy:     ep.healthy,
			Slot:        ep.slot,
			SlotLag:     p.slotLagLocked(ep),
			EWMALatency: ep.ewmaLatency,
			LatencyMS:   float64(ep.ewmaLatency) / float64(time.Millisecond),
			LastChecked: ep.lastChecked,
		}
		if ep.lastErr != nil {
			st.LastError = ep.lastErr.Error()
		}
		out = append(out, st)
	}
	return out
}

// MaxSlot returns the highest slot observed across the pool.
func (p *Pool) MaxSlot() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.maxSlot
}
