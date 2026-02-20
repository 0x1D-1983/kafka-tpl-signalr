# SignalR + Kafka EPEX Price Stream

EPEX market data pipeline: **Kafka → TPL Dataflow → SignalR**, with a React client that subscribes to symbols and receives live price updates. Built for high availability (stateless pods, Redis backplane, health checks).

## Repo layout

| Path | Description |
|------|-------------|
| [**PriceStream/**](PriceStream/README.md) | ASP.NET Core backend: Kafka consumer, TPL Dataflow pipeline, SignalR hub at `/hubs/pricestream`. Health: `/health/live`, `/health/ready`. |
| [**PriceStreamClient/**](PriceStreamClient/README.md) | React (Vite) app that connects to the hub, subscribes to symbols, and shows price updates. Auto-resubscribes on reconnect. |
| **docker-compose.yml** | Local Kafka (Zookeeper + broker) and Confluent Control Center for development. |

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

## Docs

- [PriceStream backend](PriceStream/README.md) — Architecture, configuration, Redis backplane, Kubernetes, Kafka message format.
- [PriceStreamClient](PriceStreamClient/README.md) — React app setup, proxy, and custom hub URL.
