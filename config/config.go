package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type EndpointConfig struct {
	URL    string
	Name   string
	Weight int
}

type Config struct {
	Port            string
	CORSAllowOrigin string
	Endpoints       []EndpointConfig
	HealthInterval  time.Duration
	HealthTimeout   time.Duration
	UpstreamTimeout time.Duration
	MaxSlotLag      uint64
	EWMAAlpha       float64
	FailThreshold   int
	OKThreshold     int
	MaxBodyBytes    int64
}

func Load() (Config, error) {
	var errs []error
	push := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}

	cfg := Config{
		Port:            getEnvString("PORT", "8080"),
		CORSAllowOrigin: getEnvString("CORS_ALLOW_ORIGIN", "*"),
	}

	var err error
	cfg.HealthInterval, err = getEnvDuration("HEALTH_INTERVAL", 5*time.Second)
	push(err)
	cfg.HealthTimeout, err = getEnvDuration("HEALTH_TIMEOUT", 2*time.Second)
	push(err)
	cfg.UpstreamTimeout, err = getEnvDuration("UPSTREAM_TIMEOUT", 15*time.Second)
	push(err)
	cfg.MaxSlotLag, err = getEnvUint64("MAX_SLOT_LAG", 50)
	push(err)
	cfg.EWMAAlpha, err = getEnvFloat64("EWMA_ALPHA", 0.2)
	push(err)
	cfg.FailThreshold, err = getEnvInt("FAIL_THRESHOLD", 3)
	push(err)
	cfg.OKThreshold, err = getEnvInt("OK_THRESHOLD", 2)
	push(err)
	cfg.MaxBodyBytes, err = getEnvInt64("MAX_BODY_BYTES", 5<<20) // 5 MiB
	push(err)

	eps, err := parseEndpoints(os.Getenv("RPC_ENDPOINTS"))
	push(err)
	cfg.Endpoints = eps

	push(cfg.validate())

	// errors.Join reports every problem at once. Fixing config one crash
	// at a time is miserable.
	if joined := errors.Join(errs...); joined != nil {
		return Config{}, joined
	}
	return cfg, nil
}

func parseEndpoints(raw string) ([]EndpointConfig, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, errors.New("RPC_ENDPOINTS is required (comma-separated list of RPC URLs)")
	}

	var out []EndpointConfig
	seen := map[string]int{}

	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		u, err := url.Parse(part)
		if err != nil {
			return nil, fmt.Errorf("invalid endpoint URL %q: %w", redact(part), err)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return nil, fmt.Errorf("endpoint %q: scheme must be http or https", redact(part))
		}
		if u.Hostname() == "" {
			return nil, fmt.Errorf("endpoint %q: missing host", redact(part))
		}

		// Name is derived from the hostname only. RPC URLs routinely carry
		// API keys in the path or query string; this name becomes a
		// Prometheus label and shows up in dashboards and screenshots.
		name := u.Hostname()
		seen[name]++
		if n := seen[name]; n > 1 {
			name = fmt.Sprintf("%s-%d", name, n)
		}

		out = append(out, EndpointConfig{URL: part, Name: name, Weight: 1})
	}

	if len(out) == 0 {
		return nil, errors.New("RPC_ENDPOINTS contained no usable URLs")
	}
	return out, nil
}

// redact strips everything after the host so API keys never reach logs.
func redact(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		return "<unparseable url>"
	}
	return u.Scheme + "://" + u.Host + "/…"
}

func (c Config) validate() error {
	var errs []error
	if c.HealthInterval <= 0 {
		errs = append(errs, errors.New("HEALTH_INTERVAL must be positive"))
	}
	if c.HealthTimeout <= 0 {
		errs = append(errs, errors.New("HEALTH_TIMEOUT must be positive"))
	}
	if c.HealthTimeout >= c.HealthInterval {
		// Otherwise probes from one cycle overlap the next and pile up.
		errs = append(errs, errors.New("HEALTH_TIMEOUT must be less than HEALTH_INTERVAL"))
	}
	if c.UpstreamTimeout <= 0 {
		errs = append(errs, errors.New("UPSTREAM_TIMEOUT must be positive"))
	}
	if c.EWMAAlpha <= 0 || c.EWMAAlpha > 1 {
		errs = append(errs, errors.New("EWMA_ALPHA must be in (0, 1]"))
	}
	if c.FailThreshold < 1 {
		errs = append(errs, errors.New("FAIL_THRESHOLD must be >= 1"))
	}
	if c.OKThreshold < 1 {
		errs = append(errs, errors.New("OK_THRESHOLD must be >= 1"))
	}
	if c.MaxBodyBytes < 1024 {
		errs = append(errs, errors.New("MAX_BODY_BYTES must be >= 1024"))
	}
	if c.CORSAllowOrigin == "" {
		errs = append(errs, errors.New("CORS_ALLOW_ORIGIN must not be empty (use * to allow all)"))
	}
	return errors.Join(errs...)
}

// --- env helpers ---

func getEnvString(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func getEnvDuration(key string, def time.Duration) (time.Duration, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	// time.ParseDuration accepts "5s", "250ms", "1m30s" — keeps env vars
	// self-documenting instead of bare integers with implied units.
	d, err := time.ParseDuration(v)
	if err != nil {
		return def, fmt.Errorf("%s=%q: %w", key, v, err)
	}
	return d, nil
}

func getEnvInt(key string, def int) (int, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def, fmt.Errorf("%s=%q: %w", key, v, err)
	}
	return n, nil
}

func getEnvInt64(key string, def int64) (int64, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def, fmt.Errorf("%s=%q: %w", key, v, err)
	}
	return n, nil
}

func getEnvUint64(key string, def uint64) (uint64, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return def, fmt.Errorf("%s=%q: %w", key, v, err)
	}
	return n, nil
}

func getEnvFloat64(key string, def float64) (float64, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def, fmt.Errorf("%s=%q: %w", key, v, err)
	}
	return f, nil
}