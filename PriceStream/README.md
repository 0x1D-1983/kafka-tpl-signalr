# PriceStream: Kafka → TPL Dataflow → SignalR

EPEX market data flows from Kafka through a TPL Dataflow pipeline and is exposed to clients via SignalR. Clients subscribe by symbol; reconnection is supported with automatic resubscription.

## Architecture

- **Kafka**: Single topic (e.g. `epex-market-data`). **Every pod reads every partition** and sends only to its own connections, so no backplane is needed. Partitions are assigned manually (no consumer group membership, no commits), so scaling or restarting a pod never triggers a rebalance. The cost is that Kafka read bandwidth grows with the replica count.
- **Parsing**: The Kafka value deserializer parses JSON straight from librdkafka's buffer (`ReadOnlySpan<byte>`, source-generated `JsonSerializerContext`), with no intermediate `byte[]` or `string`. Symbols are trimmed and upper-cased there; payloads that are not valid or have no symbol are skipped.
- **TPL Dataflow**: bounded queue → one fan-out block that caches the tick and hands it to each subscriber's outbox. Backpressure via bounded capacity.
- **Per-connection outbox** (`PriceSubscribers`): each connection keeps at most one pending tick per symbol and has its own sender. SignalR groups are not used, because a group send completes only when every member's write does, so one client that stops reading would stall the pipeline for everyone. A slow client instead skips to the latest price per symbol, and a send stuck longer than `SignalR:TransportSendTimeoutSeconds` (default 3) closes its connection. `SlowClientTests` checks this under load.
- **SignalR**: Hub `PriceStreamHub` at `/hubs/pricestream`. Methods: `Subscribe(symbols)`, `Unsubscribe(symbols)`, both returning `{ accepted, rejected: [{ symbol, reason }] }`. Clients receive `PriceUpdate` with EPEX payload.
- **Health**: `/health/live` (liveness: process up, pipeline not faulted), `/health/ready` (readiness: pipeline not faulted and startup replay finished), `/health/kafka` (diagnostic: broker connectivity as seen by the consumer, JSON, 503 while degraded). Use readiness for load balancer so traffic only goes to pods that can serve.
- **Snapshot on subscribe**: Each tick is stored in memory as the last price for its symbol. `Subscribe` sends the cached price as a `PriceUpdate` for each newly added symbol, so a client does not wait for the next tick. An older source timestamp never replaces a newer one.
- **Startup replay**: A new pod reads the last `ReplayWindowSeconds` of the topic into the cache without broadcasting, and stays unready until it reaches the offsets it had at startup. A client rerouted to it gets snapshots straight away.

## High availability

- **Stateless pods**: No shared state. Subscriptions are per-connection; when a client reconnects (same or different pod), it must call `Subscribe(symbols)` again.
- **Kafka**: Each pod has every symbol, so losing a pod loses no data for the others. The `group.id` is `{ConsumerGroupId}-{pod name}` and only identifies the pod; nothing is committed.
- **Load balancer**: Point liveness to `/health/live`, readiness to `/health/ready`. Sticky sessions keep negotiate and the WebSocket on the same pod. When that pod is gone, the next connection is routed to a healthy one.

## Client reconnection and resubscription

Connection state (subscribed symbols) is **not** shared across pods or persisted. When the connection drops and the client reconnects (via SignalR’s `withAutomaticReconnect()`):

1. Client should store desired symbols locally (e.g. `localStorage`).
2. On `onreconnected` (or after `start()`), call `Subscribe(symbols)` again with that list.

The **React client** (`PriceStreamClient/`) subscribes only to the symbols submitted from the textbox, then calls `Subscribe` again with that list on reconnect.

## Configuration

`appsettings.json` (or environment):

```json
"Kafka": {
  "BootstrapServers": "localhost:9092",
  "Topic": "epex-market-data",
  "ConsumerGroupId": "pricestream-signalr",
  "ReplayWindowSeconds": 900,
  "PartitionRefreshSeconds": 60
}
```

- **Kafka**: Required. Override in production (e.g. `Kafka__BootstrapServers`, `Kafka__Topic`, `Kafka__ConsumerGroupId`).
  - `ReplayWindowSeconds` (default 900): how far back a starting pod reads into the last-price cache. `0` starts at the latest offset with an empty cache.
  - `PartitionRefreshSeconds` (default 60): how often the topic is checked for added partitions, which are read from the beginning.

- **Subscriptions**: Limits on client input to the hub.
  - `AllowedSymbols`: when non-empty, only these symbols are accepted (`not_allowed`).
  - `SymbolPattern` and `MaxSymbolLength` (default 16): checked after trim and upper-casing (`invalid`).
  - `MaxSymbolsPerConnection` (default 50): distinct symbols per connection (`limit_reached`).
  - `CallsPerWindow` / `WindowSeconds` (default 10 per 10s): `Subscribe` and `Unsubscribe` share one per-connection token bucket (`rate_limited`).
  - `Unsubscribe` rejects symbols the connection is not subscribed to (`not_subscribed`).

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

Manifests under [`k8s/`](../k8s/) run three stateless replicas behind Envoy and a single-node Kafka broker. See the [root README](../README.md#kubernetes-kind--skaffold) for how to start the kind cluster.

- **Liveness probe**: `GET /health/live` → 200 = process is up and the pipeline has not faulted. A failure restarts the container.
- **Readiness probe**: `GET /health/ready` → 200 when the pipeline is healthy and the startup replay is done. The Service and Envoy only send traffic to ready pods.
- **Kafka is not in readiness or liveness.** Kafka is shared, so an outage would make every pod unready at once, and Envoy (`healthy_panic_threshold: 0`) would refuse all new connections. Instead the pods stay ready and serve cached prices (each tick carries its timestamp). The consumer reconnects on its own. `GET /health/kafka` (503 with `disconnectedSince` and `lastError`) shows the consumer's view; no probe calls it.
- **Metrics**: `GET /metrics` (Prometheus format, not routed by the edge), from `PriceStreamMetrics` plus the ASP.NET Core hosting and SignalR connection meters (`signalr_server_active_connections`):
  - `pricestream_kafka_messages_total`, `…_parsed_total`, `…_failed_total`: messages received (including replay), and whether they parsed.
  - `pricestream_messages_dropped_total{reason}`: `backpressure` (pipeline queue full), `no_symbol`, `send_failed`.
  - `pricestream_pipeline_queue_depth{stage}`: `queue`, `fanout`, and `outboxes` (pending ticks across all client outboxes).
  - `pricestream_outbox_superseded_total`: pending ticks replaced by a newer one for the same symbol before a slow client took them.
  - `pricestream_publish_latency_seconds`: histogram from the Kafka message timestamp to the send to one client completing, per delivered tick, live messages only. It includes producer `linger.ms` and clock skew, because the record timestamp is set before the producer batches.
  - `pricestream_subscriptions{symbol}`: connections on this pod per symbol; a symbol drops out at zero.
  - `pricestream_kafka_consumer_lag{topic,partition}`: high watermark minus position, updated every 5s from the consumer's last fetch.
  - `pricestream_kafka_last_message_time_seconds` (0 before the first message) and `pricestream_kafka_connected`.
- **Grafana**: the `PriceStream` dashboard ([`k8s/monitoring/dashboards/pricestream.json`](../k8s/monitoring/dashboards/pricestream.json)) is provisioned with the Prometheus datasource and is Grafana's home page. Edit the JSON, not the UI: provisioned changes made in the UI are not saved.
- **Alert `PriceStreamPodDataStale`** ([`k8s/monitoring/pricestream-rules.yaml`](../k8s/monitoring/pricestream-rules.yaml)): one pod has received nothing for over 60s while another pod received data in the last 30s, held for 2m. Because every pod reads the whole topic, that isolates a fault local to the pod (lost broker path, stuck consumer) which readiness deliberately ignores. If every pod is stale (quiet topic, Kafka outage) it stays silent. Rule tests: `promtool test rules k8s/monitoring/pricestream-rules.test.yaml`.
- **Replicas**: 3, rolling update `maxUnavailable: 0` / `maxSurge: 1`, PodDisruptionBudget `minAvailable: 2`. A deploy brings a new pod up, and lets it catch up, before taking an old one down.
- **Behind the edge**: TLS terminates at the edge, so the app does not redirect to HTTPS by default (`Https:Redirect=false`). `X-Forwarded-For` and `X-Forwarded-Proto` are honoured only from `ForwardedHeaders:KnownNetworks` / `KnownProxies` (the kind pod CIDR in the Deployment). If `Https:Redirect` is enabled, `/health/*` and `/metrics` are still never redirected, so probes and scrapes never see a 307.
- **Every replica reads all partitions.** The broker auto-creates `epex-market-data` with 3 partitions; the count is independent of the replica count.

### When a pod dies

1. The process crashes. Kubernetes restarts the container, and the dead process resets that pod's sockets, so its clients disconnect.
2. Readiness fails, so the pod leaves the headless Service. Envoy's health check (`/health/ready` every 2s, two failures) stops choosing it for new connections.
3. Clients reconnect (`withAutomaticReconnect` in the React client) and call `Subscribe` again.
4. The affinity cookie hashes to a remaining healthy pod.
5. That pod already reads every symbol: `Subscribe` sends the cached prices, and live ticks follow.

During a rolling deploy the same path runs one pod at a time, and at least two replicas stay available.
