package router

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

// writeMethods are JSON-RPC methods that must never be retried.
//
// A timeout on sendTransaction is ambiguous: the transaction may already be
// in a leader's queue. Retrying risks a double-send, which for a non-idempotent
// program means real financial loss. We fail fast and let the client — which
// knows the signature and can poll for it — decide what to do.
var writeMethods = map[string]bool{
	"sendTransaction":     true,
	"requestAirdrop":      true,
	"simulateTransaction": false, // read-only despite the name; safe to retry
}

// ProxyStats is a hook for metrics (wired up on Day 5). Keeping it as an
// interface means the hot path has no Prometheus dependency and tests can
// assert on routing decisions without scraping a registry.
type ProxyStats interface {
	ObserveRequest(endpoint, method string, status int, duration time.Duration)
	ObserveUpstreamError(endpoint, method, reason string)
	ObserveDegraded(endpoint, method string)
	ObserveRetry(fromEndpoint, toEndpoint, method string)
}

type nopStats struct{}

func (nopStats) ObserveRequest(string, string, int, time.Duration) {}
func (nopStats) ObserveUpstreamError(string, string, string)       {}
func (nopStats) ObserveDegraded(string, string)                    {}
func (nopStats) ObserveRetry(string, string, string)               {}

type Proxy struct {
	pool         *Pool
	client       *http.Client
	maxBodyBytes int64
	stats        ProxyStats
	allowOrigin  string
}

func NewProxy(pool *Pool, upstreamTimeout time.Duration, maxBodyBytes int64) *Proxy {
	// One Transport for the whole process. Transport holds the connection
	// pool; creating one per request (or per endpoint) throws away every
	// warm TLS session and is the single most common cause of "why is my
	// Go proxy slower than curl".
	transport := &http.Transport{
		// The important one. Default is 2 — under any real concurrency you
		// blow past it constantly and pay a fresh TCP + TLS handshake on
		// every excess request. To a distant RPC node that's 100-200ms
		// added to a request whose real work is 20ms.
		MaxIdleConnsPerHost: 100,
		MaxIdleConns:        200,
		IdleConnTimeout:     90 * time.Second,

		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
	}

	return &Proxy{
		pool: pool,
		client: &http.Client{
			Transport: transport,
			Timeout:   upstreamTimeout,
			// Never follow redirects. A redirect from an RPC endpoint is
			// either a misconfiguration or an attempt to send us somewhere
			// we didn't choose.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		maxBodyBytes: maxBodyBytes,
		stats:        nopStats{},
		// "*" matches what public Solana RPC endpoints do and is right for
		// an unauthenticated proxy. Anyone fronting an API-keyed upstream
		// must narrow this — "*" plus a credentialed upstream lets any
		// origin spend their quota.
		allowOrigin: "*",
	}
}

func (p *Proxy) SetStats(s ProxyStats) {
	if s != nil {
		p.stats = s
	}
}

// SetAllowOrigin overrides the CORS origin. Call before serving traffic;
// it is not safe to change concurrently with requests.
func (p *Proxy) SetAllowOrigin(origin string) {
	if origin != "" {
		p.allowOrigin = origin
	}
}

// rpcPeek extracts just enough of the JSON-RPC envelope to route on.
//
// encoding/json ignores fields not present in the target struct, so this
// decodes a 2MB request body into two small fields without materializing
// the rest. Full unmarshal-then-remarshal would cost allocation on every
// request and break whenever Solana adds a field.
type rpcPeek struct {
	Method string `json:"method"`
}

// peekMethod returns the RPC method name and whether the body is a batch.
//
// For batches we return "batch" as the method and treat it as a write —
// conservative, because a batch may contain a sendTransaction and we are
// not going to parse the whole array on the hot path to find out.
func peekMethod(body []byte) (method string, isBatch bool) {
	trimmed := bytes.TrimLeft(body, " \t\r\n")
	if len(trimmed) > 0 && trimmed[0] == '[' {
		return "batch", true
	}

	var peek rpcPeek
	if err := json.Unmarshal(trimmed, &peek); err != nil || peek.Method == "" {
		return "unknown", false
	}
	return peek.Method, false
}

func isRetryable(method string, isBatch bool) bool {
	if isBatch {
		return false
	}
	return !writeMethods[method]
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	// rpc-mesh owns its CORS policy. The upstream's headers are stripped in
	// copyResponseHeaders — relaying them means browsers see whatever origin
	// the upstream happened to send and reject the response.
	p.setCORS(w)

	// Browser dApps send a preflight OPTIONS before any cross-origin POST.
	// Without handling it the real request is never issued at all.
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if r.Method != http.MethodPost {
		writeRPCError(w, http.StatusMethodNotAllowed, nil, -32600,
			"only POST is supported for JSON-RPC")
		return
	}

	// MaxBytesReader caps the body and, unlike a manual check, stops reading
	// at the limit rather than after buffering everything. Without it, an
	// unbounded body is a trivial memory-exhaustion DoS.
	r.Body = http.MaxBytesReader(w, r.Body, p.maxBodyBytes)

	// The body must be fully buffered, not streamed: a retry needs to replay
	// it, and an io.Reader is one-shot. This is the reason MaxBodyBytes
	// exists as a config knob.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeRPCError(w, http.StatusRequestEntityTooLarge, nil, -32600,
				"request body too large")
			return
		}
		writeRPCError(w, http.StatusBadRequest, nil, -32700, "could not read request body")
		return
	}
	if len(body) == 0 {
		writeRPCError(w, http.StatusBadRequest, nil, -32700, "empty request body")
		return
	}

	method, isBatch := peekMethod(body)

	ep, degraded, err := p.pool.Select(method)
	if err != nil {
		p.stats.ObserveUpstreamError("none", method, "no_endpoints")
		writeRPCError(w, http.StatusServiceUnavailable, nil, -32603,
			"no healthy upstream endpoints")
		return
	}
	if degraded {
		// Served, but every candidate was slot-lagging. Clients may see
		// stale state; this needs to be visible in metrics and alertable.
		p.stats.ObserveDegraded(ep.Name, method)
	}

	resp, err := p.forward(r.Context(), ep, body, r)

	if err != nil && isRetryable(method, isBatch) {
		reason := classifyError(err)
		p.stats.ObserveUpstreamError(ep.Name, method, reason)

		// Don't retry if the *client* went away — that's not the upstream's
		// fault and a second attempt just wastes an endpoint's capacity.
		if r.Context().Err() == nil {
			if alt, _, selErr := p.pool.SelectExcluding(method, ep); selErr == nil {
				p.stats.ObserveRetry(ep.Name, alt.Name, method)
				log.Printf("retrying %s: %s -> %s (%s)", method, ep.Name, alt.Name, reason)
				ep = alt
				resp, err = p.forward(r.Context(), ep, body, r)
			}
		}
	}

	if err != nil {
		p.stats.ObserveUpstreamError(ep.Name, method, classifyError(err))
		status := http.StatusBadGateway
		if errors.Is(err, context.DeadlineExceeded) {
			status = http.StatusGatewayTimeout
		}
		writeRPCError(w, status, nil, -32603, "upstream request failed")
		p.stats.ObserveRequest(ep.Name, method, status, time.Since(start))
		return
	}
	defer resp.Body.Close()

	// Surface which endpoint served this. Invaluable when debugging "why did
	// this one request return stale data" — and it costs nothing.
	w.Header().Set("X-RPC-Mesh-Endpoint", ep.Name)
	if degraded {
		w.Header().Set("X-RPC-Mesh-Degraded", "true")
	}
	copyResponseHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)

	// io.Copy streams the response through without buffering it. A
	// getProgramAccounts response can be tens of megabytes; buffering would
	// spike memory proportional to concurrency for no benefit.
	if _, err := io.Copy(w, resp.Body); err != nil {
		// Headers are already sent, so we cannot convert this into an error
		// response. Log and move on.
		log.Printf("streaming response from %s: %v", ep.Name, err)
	}

	p.stats.ObserveRequest(ep.Name, method, resp.StatusCode, time.Since(start))
}

func (p *Proxy) setCORS(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Access-Control-Allow-Origin", p.allowOrigin)
	h.Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	h.Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	h.Set("Access-Control-Max-Age", "86400")
	// Without this, browser JS cannot read our custom headers even though
	// they arrive on the wire.
	h.Set("Access-Control-Expose-Headers", "X-RPC-Mesh-Endpoint, X-RPC-Mesh-Degraded")
	if p.allowOrigin != "*" {
		// Responses vary by request origin, so caches must not serve one
		// origin's response to another.
		h.Add("Vary", "Origin")
	}
}

// forward sends the buffered body to one endpoint.
func (p *Proxy) forward(ctx context.Context, ep *Endpoint, body []byte, orig *http.Request) (*http.Response, error) {
	// bytes.NewReader over the buffered body: a fresh reader per attempt,
	// which is exactly what makes retry possible.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ep.URL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("building request for %s: %w", ep.Name, err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	// ContentLength set explicitly so Go doesn't fall back to chunked
	// encoding, which some RPC providers handle poorly.
	req.ContentLength = int64(len(body))

	if ua := orig.Header.Get("User-Agent"); ua != "" {
		req.Header.Set("User-Agent", "rpc-mesh/"+ua)
	} else {
		req.Header.Set("User-Agent", "rpc-mesh")
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}

	// 5xx and 429 are upstream failures worth retrying elsewhere. 4xx is the
	// client's own fault and will fail identically on every endpoint, so it
	// passes through unretried.
	if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
		resp.Body.Close()
		return nil, fmt.Errorf("upstream %s returned %d", ep.Name, resp.StatusCode)
	}

	return resp, nil
}

// hopByHopHeaders are connection-scoped and must not be forwarded (RFC 7230).
// Forwarding them corrupts connection reuse in subtle, intermittent ways.
var hopByHopHeaders = []string{
	"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate",
	"Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade",
}

// upstreamOnlyHeaders describe the upstream origin, not ours, and must not be
// relayed.
//
//   - Alt-Svc advertises HTTP/3 on port 443 for "this origin". A browser caches
//     that and starts trying h3 against rpc-mesh's port, which does not speak
//     it — intermittent connection failures with no obvious cause.
//   - CORS headers are the upstream's policy. Public Solana endpoints send odd
//     values (one returns a literal "backend_traffic" as the allowed origin);
//     relaying them makes browsers reject otherwise-valid responses. We set
//     our own in setCORS.
//   - Allow reflects the upstream's method support, not the proxy's.
//   - Strict-Transport-Security would pin HTTPS on rpc-mesh's own host, which
//     breaks plain-HTTP local development.
var upstreamOnlyHeaders = []string{
	"Alt-Svc",
	"Access-Control-Allow-Origin",
	"Access-Control-Allow-Methods",
	"Access-Control-Allow-Headers",
	"Access-Control-Allow-Credentials",
	"Access-Control-Expose-Headers",
	"Access-Control-Max-Age",
	"Allow",
	"Strict-Transport-Security",
}

func copyResponseHeaders(dst, src http.Header) {
	for k, vv := range src {
		if isHopByHop(k) || isUpstreamOnly(k) {
			continue
		}
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

func isHopByHop(header string) bool {
	for _, h := range hopByHopHeaders {
		if strings.EqualFold(h, header) {
			return true
		}
	}
	return false
}

func isUpstreamOnly(header string) bool {
	for _, h := range upstreamOnlyHeaders {
		if strings.EqualFold(h, header) {
			return true
		}
	}
	return false
}

func classifyError(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "client_cancelled"
	case strings.Contains(err.Error(), "429"):
		return "rate_limited"
	case strings.Contains(err.Error(), "returned 5"):
		return "upstream_5xx"
	default:
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return "timeout"
		}
		return "transport"
	}
}

// writeRPCError returns a JSON-RPC 2.0 error object rather than a plain HTTP
// error. Solana clients parse the body; an HTML or text error surfaces as an
// unhelpful "unexpected token" deep inside web3.js.
func writeRPCError(w http.ResponseWriter, httpStatus int, id json.RawMessage, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)

	if id == nil {
		id = json.RawMessage("null")
	}
	resp := struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Error   struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}{JSONRPC: "2.0", ID: id}
	resp.Error.Code = code
	resp.Error.Message = msg

	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("writing rpc error: %v", err)
	}
}