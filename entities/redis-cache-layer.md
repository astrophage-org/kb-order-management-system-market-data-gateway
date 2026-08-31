<!-- anchor: docs/architecture.md:L1-L100 sha:HEAD -->

# Redis Cache Layer

The **Redis Cache Layer** serves as the high-performance, in-memory state repository for the [[summaries/market-data-gateway|Market Data Gateway (MDG)]]. By caching the latest [[concepts/level-2-depth|Level 2 (L2) order book]] snapshots, it provides an instantaneous state-recovery mechanism and a warm-start bootstrapping interface for newly connected WebSocket clients. This mitigates the latency and bandwidth overhead of waiting for the next scheduled broadcast event streamed over the [[entities/kafka-message-bus|Kafka Message Bus]].

---

## Responsibilities

* **L2 Snapshot Caching**: Persist the most recent [[concepts/level-2-depth|Level 2 order book]] state for every active trading symbol supported by the Nexus Trading Exchange (NTE).
* **Warm-Start Bootstrapping**: Deliver immediate state snapshots to the [[entities/client-manager|Client Manager]] when external clients establish new [[concepts/websocket-egress|WebSocket connections]], avoiding "cold start" visibility gaps.
* **State Recovery Integration**: Enable the [[entities/order-book-consumer|Order Book Consumer]] and other gateway components to rapidly recover their local memory space during application restarts or container failovers without putting stress on the upstream [[entities/order-matching-engine|Order Matching Engine]].
* **Downstream Load Reduction**: Prevent the need to query the transactional database or the core order matching engine for public-facing read-heavy query patterns.

---

## Dependencies

* **Upstream Data Ingress**: Relies on the [[entities/order-book-consumer|Order Book Consumer]] to ingest updates from the [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-orderbook-snapshots|order-matching-engine (nte.orderbook.snapshots)]] Kafka topic, translate/parse the binary [[entities/protobuf-schema|Protocol Buffers]], and push them to Redis.
* **Downstream Consumers**: Interfaced directly by the [[entities/client-manager|Client Manager]] to fetch cached data for newly initialized client sockets.
* **Infrastructure**: Deployed as an adjacent memory-optimized cache container (`redis:alpine`) as specified in the `docker-compose.yml` infrastructure topology.
* **Architectural Decisions**: Operates under the caching strategies outlined in [[decisions/state-ingestion-recovery]] and the translation models of [[decisions/protocol-translation]].

---

## Data Schema & Key Design

Redis keys are designed to provide $O(1)$ lookups for active order book states.

### Key Structure
| Key Pattern | Data Type | Payload Type | Description |
| :--- | :--- | :--- | :--- |
| `nte:orderbook:snapshot:<symbol>` | String | JSON (or serialized Protobuf) | Stores the latest complete bids and asks depth representation for a given market symbol. |

### Payload Contract
The cached value corresponds directly to the `OrderBookSnapshot` schema specified in [[entities/protobuf-schema|Protobuf Format]] or its parsed JSON representation inside [[concepts/data-flow-pipelines|Data Flow Pipelines]]:

```json
{
  "symbol": "BTCUSD",
  "bids": [
    {"price": 64250.5, "quantity": 1.42, "orderCount": 3},
    {"price": 64249.0, "quantity": 3.85, "orderCount": 8}
  ],
  "asks": [
    {"price": 64251.0, "quantity": 0.95, "orderCount": 2},
    {"price": 64252.5, "quantity": 2.11, "orderCount": 5}
  ],
  "timestamp": 1698246321000
}
```

---

## Warm-Start & Recovery Workflow

```
 ┌──────────────┐         ┌─────────────────┐         ┌─────────────────┐
 │ New Client   │         │ Client Manager  │         │   Redis Cache   │
 └──────┬───────┘         └────────┬────────┘         └────────┬────────┘
        │                          │                           │
        │── Websocket Connect ────>│                           │
        │                          │── Read Latest Snapshot ──>│
        │                          │   (nte:orderbook:...)     │
        │                          │<── Return Snapshot ───────│
        │<── Push L2_UPDATE ───────│                           │
        │    (Warm-Start State)    │                           │
        │                          │                           │
```

1. **Client Handshake**: An external subscriber establishes a WebSocket session handled by the [[entities/client-manager|Client Manager]].
2. **Cache Fetch**: Before subscribing the client to the real-time event loop (which would introduce latency jitter as detailed in [[decisions/sequential-broadcast-fairness]]), the Client Manager performs an asynchronous `GET` request to Redis for the active symbols.
3. **Immediate Serialization**: The Client Manager pushes the cached snapshot over [[concepts/websocket-egress|WebSocket Egress]] as an `L2_UPDATE` event.
4. **Subsequent Stream**: The client's connection is then registered into the streaming broadcast pool to receive live delta updates pushed in real-time from the Kafka brokers.

This decoupled architecture ensures the gateway remains highly responsive, adhering to [[decisions/decoupling-via-pub-sub]] by isolating the matching engine core from client connection volatility.