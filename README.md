## Initial RPC mesh routing engine

- Round-robin load balancer
- Health check polling
- Prometheus metrics export
- Support for Devnet/Testnet/Mainnet

## 📁 Repo Structure

```
rpc-mesh/
├── main.go                      # Entry point
├── go.mod / go.sum              # Dependencies
├── router/
│   ├── load_balancer.go         # Core routing logic
│   ├── health_check.go          # Endpoint health monitoring
│   └── types.go                 # Data structures
├── metrics/
│   └── prometheus.go            # Metrics collection
├── config/
│   └── config.go                # Configuration
├── tests/
│   └── *_test.go                # Unit tests
├── Dockerfile                   # Container image
├── docker-compose.yml           # Local dev environment
├── Makefile                     # Build commands
└── README.md                    # This file
```
