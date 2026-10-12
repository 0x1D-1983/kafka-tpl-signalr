# SignalR + Kafka EPEX Price Stream

EPEX market data pipeline: **Kafka → TPL Dataflow → SignalR**, with a React client that subscribes to symbols and receives live price updates. Built for high availability (stateless pods, Redis backplane, health checks).

## Repo layout

| Path | Description |
|------|-------------|
| [**PriceStream/**](PriceStream/README.md) | ASP.NET Core backend: Kafka consumer, TPL Dataflow pipeline, SignalR hub at `/hubs/pricestream`. Health: `/health/live`, `/health/ready`. |
| [**PriceStreamClient/**](PriceStreamClient/README.md) | React (Vite) app that connects to the hub, subscribes to symbols, and shows price updates. Auto-resubscribes on reconnect. |
| **docker-compose.yml** | Local Kafka (Zookeeper + broker) and Confluent Control Center for development. |
| [**k8s/**](k8s/) | kind manifests: 3 PriceStream replicas, Envoy edge, Redis backplane, Kafka. |
| **skaffold.yaml** | Build the PriceStream image and deploy the manifests. |

## Quick start

1. **Start Kafka** (and optional Control Center):
   ```bash
   docker compose up -d
   ```
   Create the topic (e.g. from Control Center at http://localhost:9021 or CLI):
   ```bash
   docker exec broker kafka-topics --create --bootstrap-server localhost:9092 --topic epex-market-data --partitions 3 --replication-factor 1
   ```

2. **Run the backend** (from repo root):
   ```bash
   cd PriceStream && dotnet run
   ```
   Hub: `http://localhost:5044/hubs/pricestream` (or the port in your launchSettings).

3. **Run the React client**:
   ```bash
   cd PriceStreamClient && npm install && npm run dev
   ```
   Open http://localhost:5173, subscribe to symbols (e.g. `DE`, `FR`).

4. **Produce test messages** to `epex-market-data` (see `PriceStream/sample-epex-messages.json` for JSON format). Updates appear in the React app.

## Kubernetes (kind + Skaffold)

Requirements: Docker, [kind](https://kind.sigs.k8s.io/), kubectl and [Skaffold](https://skaffold.dev/) v2 (`brew install kind kubectl skaffold`).

```
k8s/
  namespace.yaml                 pricestream namespace
  kind-cluster.yaml              kind cluster; edge NodePort published as localhost:8080
  pricestream/pricestream.yaml   3 stateless replicas, probes, PDB, NetworkPolicy
  redis/redis.yaml               SignalR backplane (pub/sub)
  kafka/kafka.yaml               single-node KRaft broker, 3 partitions
  mock/                          publishes mock EpexMarketData for delivery areas DE and FR
  edge/                          Envoy: /hubs/* only, cookie affinity, active health checks
scripts/cluster-up.sh            creates the cluster
skaffold.yaml                    build + deploy
```

```bash
./scripts/cluster-up.sh   # creates the "pricestream" kind cluster and switches kubectl to kind-pricestream
skaffold run              # one-off build and deploy (skaffold dev: rebuild on change and stream logs)
```

Everything runs in the `pricestream` namespace. The public entry point is the edge Envoy on <http://localhost:8080>. Only `/hubs/*` is routed; `/health/*` is not public (probes call the pod IP directly).

```bash
curl -si -X POST "localhost:8080/hubs/pricestream/negotiate?negotiateVersion=1"
curl -si localhost:8080/health/ready                 # 404 from Envoy: not routed
kubectl port-forward svc/pricestream 8081:80         # skaffold dev does this; /health/* is here
```

The `epex-mock` deployment publishes a DE and an FR price every second (JSON shape in `PriceStream/sample-epex-messages.json`). To publish one message by hand:

```bash
kubectl exec -i deploy/kafka -- /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic epex-market-data <<'EOF'
{"Symbol":"DE","Market":"Day-Ahead","DeliveryArea":"DE-LU","Timestamp":"2026-10-12T12:00:00+00:00","Price":42.35,"Unit":"EUR/MWh","Contract":"P60"}
EOF
```

Three pods share the `pricestream-signalr` consumer group. Envoy sticks a client to one pod for the negotiate request and the WebSocket. Redis pub/sub delivers symbol groups to every pod, so a client still receives symbols consumed elsewhere. If that pod crashes or is drained, Envoy removes it and the React client reconnects onto another ready pod, then resubscribes.

Point the React dev server at the edge:

```bash
cd PriceStreamClient && VITE_PROXY_TARGET=http://localhost:8080 npm run dev
```

Cleanup:

```bash
skaffold delete
kind delete cluster --name pricestream
```

## Docs

- [PriceStream backend](PriceStream/README.md) — Architecture, configuration, Redis backplane, Kubernetes, Kafka message format.
- [PriceStreamClient](PriceStreamClient/README.md) — React app setup, proxy, and custom hub URL.
