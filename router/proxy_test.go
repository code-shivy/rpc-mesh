package router

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fakeRPC is a stand-in upstream. Configurable so tests can express
// "this endpoint is broken" or "this one is slow" directly.
type fakeRPC struct {
	server   *httptest.Server
	requests atomic.Int32
	status   atomic.Int32
	delay    atomic.Int64 // nanoseconds
}

func newFakeRPC(t *testing.T, name string) *fakeRPC {
	t.Helper()
	f := &fakeRPC{}
	f.status.Store(http.StatusOK)

	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.requests.Add(1)

		if d := time.Duration(f.delay.Load()); d > 0 {
			select {
			case <-time.After(d):
			case <-r.Context().Done():
				return
			}
		}

		code := int(f.status.Load())
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		fmt.Fprintf(w, `{"jsonrpc":"2.0","id":1,"result":"%s"}`, name)
	}))
	t.Cleanup(f.server.Close)
	return f
}

func newTestProxy(t *testing.T, pool *Pool) *Proxy {
	t.Helper()
	return NewProxy(pool, 2*time.Second, 5<<20)
}

func post(t *testing.T, p *Proxy, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	return rec
}

func TestProxyRoutesToFastestEndpoint(t *testing.T) {
	fast := newFakeRPC(t, "fast")
	slow := newFakeRPC(t, "slow")

	fastEP := ep("fast", true, 1000, 10*time.Millisecond)
	fastEP.URL = fast.server.URL
	slowEP := ep("slow", true, 1000, 500*time.Millisecond)
	slowEP.URL = slow.server.URL

	p := testPool(t, slowEP, fastEP)
	p.maxSlot = 1000

	rec := post(t, newTestProxy(t, p), `{"jsonrpc":"2.0","id":1,"method":"getBalance"}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("X-RPC-Mesh-Endpoint"); got != "fast" {
		t.Errorf("served by %q, want fast", got)
	}
	if slow.requests.Load() != 0 {
		t.Error("slow endpoint received traffic")
	}
}

func TestProxyRetriesReadOnDifferentEndpoint(t *testing.T) {
	broken := newFakeRPC(t, "broken")
	broken.status.Store(http.StatusInternalServerError)
	good := newFakeRPC(t, "good")

	brokenEP := ep("broken", true, 1000, 10*time.Millisecond)
	brokenEP.URL = broken.server.URL
	goodEP := ep("good", true, 1000, 100*time.Millisecond)
	goodEP.URL = good.server.URL

	p := testPool(t, brokenEP, goodEP)
	p.maxSlot = 1000

	rec := post(t, newTestProxy(t, p), `{"jsonrpc":"2.0","id":1,"method":"getBalance"}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 after retry", rec.Code)
	}
	if got := rec.Header().Get("X-RPC-Mesh-Endpoint"); got != "good" {
		t.Errorf("served by %q, want good", got)
	}
	if broken.requests.Load() != 1 || good.requests.Load() != 1 {
		t.Errorf("requests: broken=%d good=%d, want 1 and 1",
			broken.requests.Load(), good.requests.Load())
	}
}

func TestProxyNeverRetriesSendTransaction(t *testing.T) {
	// The most important test in the file. A retried sendTransaction can
	// double-send; the failure must surface to the client instead.
	broken := newFakeRPC(t, "broken")
	broken.status.Store(http.StatusInternalServerError)
	good := newFakeRPC(t, "good")

	brokenEP := ep("broken", true, 1000, 10*time.Millisecond)
	brokenEP.URL = broken.server.URL
	goodEP := ep("good", true, 1000, 100*time.Millisecond)
	goodEP.URL = good.server.URL

	p := testPool(t, brokenEP, goodEP)
	p.maxSlot = 1000

	rec := post(t, newTestProxy(t, p), `{"jsonrpc":"2.0","id":1,"method":"sendTransaction"}`)

	if rec.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", rec.Code)
	}
	if good.requests.Load() != 0 {
		t.Fatal("sendTransaction was retried — this risks a double-send")
	}
}

func TestProxyBatchIsNotRetried(t *testing.T) {
	// A batch may contain a sendTransaction; we don't parse the array on
	// the hot path, so we treat batches conservatively.
	broken := newFakeRPC(t, "broken")
	broken.status.Store(http.StatusInternalServerError)
	good := newFakeRPC(t, "good")

	brokenEP := ep("broken", true, 1000, 10*time.Millisecond)
	brokenEP.URL = broken.server.URL
	goodEP := ep("good", true, 1000, 100*time.Millisecond)
	goodEP.URL = good.server.URL

	p := testPool(t, brokenEP, goodEP)
	p.maxSlot = 1000

	rec := post(t, newTestProxy(t, p),
		`[{"jsonrpc":"2.0","id":1,"method":"getBalance"},{"jsonrpc":"2.0","id":2,"method":"sendTransaction"}]`)

	if rec.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", rec.Code)
	}
	if good.requests.Load() != 0 {
		t.Fatal("batch was retried")
	}
}

func TestProxyReturnsJSONRPCErrorWhenNoEndpoints(t *testing.T) {
	down := ep("down", false, 1000, 10*time.Millisecond)
	p := testPool(t, down)
	p.maxSlot = 1000

	rec := post(t, newTestProxy(t, p), `{"jsonrpc":"2.0","id":1,"method":"getBalance"}`)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}

	// Must be a parseable JSON-RPC error, not plain text — otherwise
	// web3.js surfaces an opaque JSON parse failure.
	var body struct {
		JSONRPC string `json:"jsonrpc"`
		Error   struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("response is not valid JSON: %v", err)
	}
	if body.JSONRPC != "2.0" || body.Error.Code == 0 {
		t.Errorf("malformed JSON-RPC error: %+v", body)
	}
}

func TestProxyRejectsOversizedBody(t *testing.T) {
	backend := newFakeRPC(t, "backend")
	e := ep("backend", true, 1000, 10*time.Millisecond)
	e.URL = backend.server.URL

	p := testPool(t, e)
	p.maxSlot = 1000

	proxy := NewProxy(p, 2*time.Second, 100) // 100-byte cap
	rec := post(t, proxy, `{"jsonrpc":"2.0","id":1,"method":"getBalance","params":["`+
		strings.Repeat("A", 500)+`"]}`)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413", rec.Code)
	}
	if backend.requests.Load() != 0 {
		t.Error("oversized request reached upstream")
	}
}

func TestProxyRejectsGET(t *testing.T) {
	e := ep("a", true, 1000, 10*time.Millisecond)
	p := testPool(t, e)
	p.maxSlot = 1000

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	newTestProxy(t, p).ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", rec.Code)
	}
}

func TestPeekMethod(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		wantMethod  string
		wantBatch   bool
		wantRetry   bool
	}{
		{"simple read", `{"jsonrpc":"2.0","id":1,"method":"getBalance"}`, "getBalance", false, true},
		{"write", `{"jsonrpc":"2.0","id":1,"method":"sendTransaction"}`, "sendTransaction", false, false},
		{"simulate is a read", `{"method":"simulateTransaction"}`, "simulateTransaction", false, true},
		{"leading whitespace", "  \n\t" + `{"method":"getSlot"}`, "getSlot", false, true},
		{"batch", `[{"method":"getBalance"}]`, "batch", true, false},
		{"malformed", `not json`, "unknown", false, true},
		{"method after big params", `{"params":["` + strings.Repeat("x", 1000) + `"],"method":"getSlot"}`, "getSlot", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			method, batch := peekMethod([]byte(tt.body))
			if method != tt.wantMethod {
				t.Errorf("method = %q, want %q", method, tt.wantMethod)
			}
			if batch != tt.wantBatch {
				t.Errorf("isBatch = %v, want %v", batch, tt.wantBatch)
			}
			if got := isRetryable(method, batch); got != tt.wantRetry {
				t.Errorf("isRetryable = %v, want %v", got, tt.wantRetry)
			}
		})
	}
}

func TestProxyPropagatesClientCancellation(t *testing.T) {
	backend := newFakeRPC(t, "slow")
	backend.delay.Store(int64(2 * time.Second))

	e := ep("slow", true, 1000, 10*time.Millisecond)
	e.URL = backend.server.URL
	p := testPool(t, e)
	p.maxSlot = 1000

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/",
		strings.NewReader(`{"method":"getBalance"}`)).WithContext(ctx)
	rec := httptest.NewRecorder()

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	newTestProxy(t, p).ServeHTTP(rec, req)

	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("took %v — client cancellation not propagated upstream", elapsed)
	}
}

var _ = io.Discard // keep io imported for future streaming assertions