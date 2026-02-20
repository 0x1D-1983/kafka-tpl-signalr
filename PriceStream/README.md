# PriceStream: Kafka → TPL Dataflow → SignalR

EPEX market data flows from Kafka through a TPL Dataflow pipeline and is exposed to clients via SignalR. Clients subscribe by symbol; reconnection is supported with automatic resubscription.

## Architecture

- **Kafka**: Single topic (e.g. `epex-market-data`). Each pod runs a consumer in the **same consumer group** so partitions are distributed across pods (no single consumer).
- **TPL Dataflow**: Parse JSON → broadcast to in-process subscribers. Backpressure via bounded capacity.
- **SignalR**: Hub `PriceStreamHub` at `/hubs/pricestream`. Methods: `Subscribe(symbols)`, `Unsubscribe(symbols)`. Clients receive `PriceUpdate` with EPEX payload. With multiple pods, use the **Redis backplane** (see below) so a client subscribed to two symbols can receive both when each symbol is consumed by a different pod.
- **Redis backplane** (optional): When `Redis:Configuration` is set, SignalR uses Redis to broadcast group messages across pods. Pod A may consume Kafka for symbol DE, Pod B for FR; with the backplane, a client connected to Pod A and subscribed to DE and FR still receives FR updates (from Pod B via Redis).
- **Health**: `/health/live` (liveness), `/health/ready` (readiness: Kafka, and Redis when backplane is enabled). Use readiness for load balancer so traffic only goes to pods that can serve.

## High availability

- **Stateless pods**: No in-memory shared state. Subscriptions are per-connection; when a client reconnects (same or different pod), it must call `Subscribe(symbols)` again.
- **Kafka consumer group**: Same `ConsumerGroupId` on all pods so the topic is partitioned across instances. No single point of failure for consumption.
- **Load balancer**: Point liveness to `/health/live`, readiness to `/health/ready`. Use sticky sessions for WebSocket (SignalR) if your LB supports it, so a reconnecting client can land on the same pod when possible.

## Client reconnection and resubscription

Connection state (subscribed symbols) is **not** shared across pods or persisted. When the connection drops and the client reconnects (via SignalR’s `withAutomaticReconnect()`):

1. Client should store desired symbols locally (e.g. `localStorage`).
2. On `onreconnected` (or after `start()`), call `Subscribe(symbols)` again with that list.

The **React client** (`PriceStreamClient/`) does this: it saves symbols to `localStorage` and resubscribes on reconnect.

## Configuration

`appsettings.json` (or environment):

```json
"Kafka": {
  "BootstrapServers": "localhost:9092",
  "Topic": "epex-market-data",
  "ConsumerGroupId": "pricestream-signalr"
},
"Redis": {
  "Configuration": "localhost:6379",
  "InstanceName": "pricestream"
}
```

- **Kafka**: Required. Override in production (e.g. `Kafka__BootstrapServers`, `Kafka__Topic`, `Kafka__ConsumerGroupId`).
- **Redis**: Optional. If `Redis:Configuration` is set, the SignalR Redis backplane is enabled so clients on one pod receive updates for symbols consumed on other pods. Use `InstanceName` to isolate multiple apps sharing the same Redis (channel prefix). Override with `Redis__Configuration`, `Redis__InstanceName`.

## Kafka message format

JSON per message, matching `EpexMarketData`:

- `Symbol` (string)
- `Market` (string)
- `DeliveryArea` (string)
- `Timestamp` (ISO date)
- `Price` (number)
- `Unit`, `Contract` (optional)

## Running locally

1. Start Kafka and create topic `epex-market-data`.
2. Run the backend: from `PriceStream/`, `dotnet run`. The hub is at `http://localhost:5044/hubs/pricestream` (or the port in your launchSettings).
3. Run the **React client**: from `PriceStreamClient/`, `npm install` then `npm run dev`. Open http://localhost:5173, subscribe to symbols (e.g. `DE`, `FR`). The dev server proxies `/hubs` to the backend.
4. Produce test messages to the Kafka topic in the same JSON shape (see `sample-epex-messages.json`) to see updates in the React app.

## Kubernetes

- **Liveness probe**: `GET /health/live` → 200 = healthy.
- **Readiness probe**: `GET /health/ready` → 200 when Kafka (and Redis, if backplane enabled) is reachable; use for Service/Ingress so only ready pods receive traffic.
- Use the same `ConsumerGroupId` for all replicas so partition assignment is shared and there is no single point of failure.
- When running multiple pods and clients subscribe to multiple symbols, set `Redis:Configuration` so the Redis backplane is enabled; otherwise a client may only receive updates for symbols consumed by the pod it is connected to.
