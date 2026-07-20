package router

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func newTestChecker(t *testing.T, pool *Pool, probe ProbeFunc) *HealthChecker {
	t.Helper()
	return &HealthChecker{
		pool:     pool,
		interval: 10 * time.Millisecond,
		timeout:  50 * time.Millisecond,
		probe:    probe,
		clients:  nil, // unused when probe is injected
	}
}

func TestRunCycleDetectsSlotLag(t *testing.T) {
	current := ep("current", true, 0, 0)
	behind := ep("behind", true, 0, 0)
	p := testPool(t, current, behind)

	hc := newTestChecker(t, p, func(_ context.Context, e *Endpoint) (uint64, error) {
		if e.Name == "current" {
			return 200_000_000, nil
		}
		return 200_000_000 - 500, nil // 500 slots behind, threshold is 50
	})

	hc.RunCycle(context.Background())

	got, degraded, err := p.Select("getBalance")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Name != "current" {
		t.Errorf("selected %q, want current", got.Name)
	}
	if degraded {
		t.Error("degraded = true, want false — a current endpoint exists")
	}
}

func TestRunCycleProbesInParallel(t *testing.T) {
	// Four endpoints, each probe sleeping 100ms. Sequential would be 400ms.
	eps := []*Endpoint{
		ep("a", true, 0, 0), ep("b", true, 0, 0),
		ep("c", true, 0, 0), ep("d", true, 0, 0),
	}
	p := testPool(t, eps...)

	var concurrent, maxConcurrent atomic.Int32
	hc := newTestChecker(t, p, func(ctx context.Context, e *Endpoint) (uint64, error) {
		n := concurrent.Add(1)
		for {
			m := maxConcurrent.Load()
			if n <= m || maxConcurrent.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
		concurrent.Add(-1)
		return 1000, nil
	})

	start := time.Now()
	hc.RunCycle(context.Background())
	elapsed := time.Since(start)

	if elapsed > 250*time.Millisecond {
		t.Errorf("cycle took %v, expected ~100ms — probes are not parallel", elapsed)
	}
	if maxConcurrent.Load() != 4 {
		t.Errorf("max concurrency was %d, want 4", maxConcurrent.Load())
	}
}

func TestProbeTimeoutIsEnforced(t *testing.T) {
	e := ep("hung", true, 0, 0)
	p := testPool(t, e)

	// Simulates a hung endpoint: blocks until its context deadline fires.
	hc := newTestChecker(t, p, func(ctx context.Context, _ *Endpoint) (uint64, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	})

	start := time.Now()
	hc.RunCycle(context.Background())
	elapsed := time.Since(start)

	if elapsed > 200*time.Millisecond {
		t.Errorf("cycle took %v, timeout is 50ms — deadline not enforced", elapsed)
	}
	if e.consecFails != 1 {
		t.Errorf("consecFails = %d, want 1", e.consecFails)
	}
}

func TestRunStopsOnContextCancel(t *testing.T) {
	e := ep("a", true, 0, 0)
	p := testPool(t, e)

	var cycles atomic.Int32
	hc := newTestChecker(t, p, func(context.Context, *Endpoint) (uint64, error) {
		cycles.Add(1)
		return 1000, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		hc.Run(ctx)
	}()

	time.Sleep(35 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after context cancellation")
	}

	if cycles.Load() < 2 {
		t.Errorf("ran %d cycles in 35ms at a 10ms interval, expected several", cycles.Load())
	}
}

func TestEjectedEndpointRecovers(t *testing.T) {
	e := ep("flaky", true, 0, 0)
	p := testPool(t, e)

	var failing atomic.Bool
	failing.Store(true)

	hc := newTestChecker(t, p, func(context.Context, *Endpoint) (uint64, error) {
		if failing.Load() {
			return 0, errors.New("connection refused")
		}
		return 1000, nil
	})

	for i := 0; i < 3; i++ {
		hc.RunCycle(context.Background())
	}
	if _, _, err := p.Select(""); !errors.Is(err, ErrNoEndpoints) {
		t.Fatal("endpoint should be ejected after 3 failed cycles")
	}

	failing.Store(false)
	hc.RunCycle(context.Background())
	if _, _, err := p.Select(""); !errors.Is(err, ErrNoEndpoints) {
		t.Fatal("should still be ejected after 1 success, threshold is 2")
	}

	hc.RunCycle(context.Background())
	if _, _, err := p.Select(""); err != nil {
		t.Fatalf("should be readmitted after 2 successes: %v", err)
	}
}