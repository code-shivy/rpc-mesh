package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/shubham-astro/rpc-mesh/config"
	"github.com/shubham-astro/rpc-mesh/router"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config error:\n%v", err)
	}

	pool, err := router.NewPool(cfg)
	if err != nil {
		log.Fatalf("pool error: %v", err)
	}
	// NotifyContext cancels ctx on SIGINT/SIGTERM. Container orchestrators
	// send SIGTERM then SIGKILL after a grace period — draining in between
	// is what makes deploys not drop requests.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	hc := router.NewHealthChecker(pool, cfg.HealthInterval, cfg.HealthTimeout)

	// hcDone lets shutdown wait for the checker to finish its current cycle.
	// A closed channel is the idiomatic "this is finished" broadcast: any
	// number of receivers unblock, and it can't be signalled twice by accident.
	hcDone := make(chan struct{})
	go func() {
		defer close(hcDone)
		hc.Run(ctx)
	}()

	mux := http.NewServeMux()

	// Liveness: is this process up? Deliberately does NOT depend on upstream
	// health — if all upstreams are down, the orchestrator restarting
	// rpc-mesh fixes nothing and just adds churn.
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok\n"))
	})

	// "Should a load balancer send us traffic?" With no healthy endpoint we
	// can serve nothing useful, so we shed traffic without dying.
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		if _, _, err := pool.Select(""); err != nil {
			http.Error(w, "no healthy endpoints", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ready\n"))
	})

	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		resp := struct {
			MaxSlot   uint64                 `json:"max_slot"`
			Endpoints []router.EndpointState `json:"endpoints"`
		}{
			MaxSlot:   pool.MaxSlot(),
			Endpoints: pool.Snapshot(),
		}
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		if err := enc.Encode(resp); err != nil {
			log.Printf("status encode: %v", err)
		}
	})

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       90 * time.Second,
	}

	go func() {
		log.Printf("rpc-mesh listening on :%s with %d endpoint(s)", cfg.Port, len(cfg.Endpoints))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutdown signal received, draining…")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
		os.Exit(1)
	}
	// Order matters. Stopping the checker first would leave in-flight
	// requests routing on health data that has stopped updating.
	select {
	case <-hcDone:
	case <-time.After(3 * time.Second):
		log.Println("health checker did not stop in time")
	}

	log.Println("stopped cleanly")
}