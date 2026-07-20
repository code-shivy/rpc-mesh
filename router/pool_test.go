package router

import (
	"errors"
	"testing"
	"time"

	"github.com/shubham-astro/rpc-mesh/config"
)

func testPool(t *testing.T, eps ...*Endpoint) *Pool {
	t.Helper()
	return &Pool{
		endpoints:  eps,
		maxSlotLag: 50,
		ewmaAlpha:  0.2,
		failThresh: 3,
		okThresh:   2,
	}
}

func ep(name string, healthy bool, slot uint64, latency time.Duration) *Endpoint {
	return &Endpoint{
		URL:         "https://" + name + ".example.com",
		Name:        name,
		healthy:     healthy,
		slot:        slot,
		ewmaLatency: latency,
	}
}

func TestSelectExcluding(t *testing.T) {
	fast := ep("fast", true, 1000, 20*time.Millisecond)
	slow := ep("slow", true, 1000, 200*time.Millisecond)
	down := ep("down", false, 1000, 5*time.Millisecond)
	lagging := ep("lagging", true, 900, 5*time.Millisecond)

	tests := []struct {
		name         string
		endpoints    []*Endpoint
		maxSlot      uint64
		exclude      *Endpoint
		wantName     string
		wantDegraded bool
		wantErr      error
	}{
		{
			name:      "all healthy picks fastest",
			endpoints: []*Endpoint{slow, fast},
			maxSlot:   1000,
			wantName:  "fast",
		},
		{
			name:      "unhealthy skipped even if fastest",
			endpoints: []*Endpoint{down, fast, slow},
			maxSlot:   1000,
			wantName:  "fast",
		},
		{
			name:      "excluded endpoint skipped on retry",
			endpoints: []*Endpoint{fast, slow},
			maxSlot:   1000,
			exclude:   fast,
			wantName:  "slow",
		},
		{
			name:      "lagging endpoint skipped when a current one exists",
			endpoints: []*Endpoint{lagging, slow},
			maxSlot:   1000,
			wantName:  "slow",
		},
		{
			name:         "all lagging degrades rather than failing",
			endpoints:    []*Endpoint{lagging},
			maxSlot:      1000,
			wantName:     "lagging",
			wantDegraded: true,
		},
		{
			name:      "no healthy endpoints returns error",
			endpoints: []*Endpoint{down},
			maxSlot:   1000,
			wantErr:   ErrNoEndpoints,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := testPool(t, tt.endpoints...)
			p.maxSlot = tt.maxSlot

			got, degraded, err := p.SelectExcluding("getSlot", tt.exclude)

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Name != tt.wantName {
				t.Errorf("selected %q, want %q", got.Name, tt.wantName)
			}
			if degraded != tt.wantDegraded {
				t.Errorf("degraded = %v, want %v", degraded, tt.wantDegraded)
			}
		})
	}
}

func TestUnprobedEndpointDoesNotBeatMeasuredOne(t *testing.T) {
	// Regression guard: ewmaLatency == 0 must mean "unknown", not "instant".
	measured := ep("measured", true, 1000, 50*time.Millisecond)
	unprobed := ep("unprobed", true, 1000, 0)

	p := testPool(t, unprobed, measured)
	p.maxSlot = 1000

	got, _, err := p.SelectExcluding("getSlot", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Name != "measured" {
		t.Errorf("selected %q, want measured", got.Name)
	}
}

func TestHysteresis(t *testing.T) {
	e := ep("flaky", true, 1000, 10*time.Millisecond)
	p := testPool(t, e)
	boom := errors.New("timeout")

	for i := 1; i <= 2; i++ {
		p.RecordProbe(ProbeResult{Endpoint: e, Err: boom})
		if !e.healthy {
			t.Fatalf("ejected after %d failures, threshold is 3", i)
		}
	}

	p.RecordProbe(ProbeResult{Endpoint: e, Err: boom})
	if e.healthy {
		t.Fatal("should be ejected after 3 consecutive failures")
	}

	p.RecordProbe(ProbeResult{Endpoint: e, Slot: 1001, Latency: 10 * time.Millisecond})
	if e.healthy {
		t.Fatal("readmitted after 1 success, threshold is 2")
	}

	p.RecordProbe(ProbeResult{Endpoint: e, Slot: 1002, Latency: 10 * time.Millisecond})
	if !e.healthy {
		t.Fatal("should be readmitted after 2 consecutive successes")
	}
}

func TestEWMASeeding(t *testing.T) {
	e := ep("new", true, 0, 0)
	p := testPool(t, e)

	p.RecordProbe(ProbeResult{Endpoint: e, Slot: 100, Latency: 100 * time.Millisecond})
	if e.ewmaLatency != 100*time.Millisecond {
		t.Fatalf("first sample should seed directly, got %v", e.ewmaLatency)
	}

	// 0.2*200 + 0.8*100 = 120ms
	p.RecordProbe(ProbeResult{Endpoint: e, Slot: 101, Latency: 200 * time.Millisecond})
	if e.ewmaLatency != 120*time.Millisecond {
		t.Fatalf("ewma = %v, want 120ms", e.ewmaLatency)
	}
}