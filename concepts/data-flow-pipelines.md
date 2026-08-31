# End-to-End Ingestion and Distribution Data Pipelines

The **Market Data Gateway (MDG)** is engineered as a low-latency, high-throughput asynchronous pipeline framework that ingests upstream trading events and distributes them to external clients. This document details the end-to-end data pipelines for **Level 2 (L2) Order Book Snapshots** and **Trade Tick Executions**, highlighting how ingestion, deserialization, caching, and egress distribution operate in concert.

For an architectural overview of the entire system, refer to the [[index]] and the [[summaries/market-data-gateway]].

---

## 1. Pipeline High-Level Flow

To insulate the core trading engine from external internet-facing network conditions and spikes in client subscriptions, the MDG utilizes an asynchronous, pub-sub model. The boundary between the internal high-performance network and the external client distribution network is managed via the [[entities/kafka-message-bus]], which serves as our decoupling layer.

```
+───────────────────────────────+
|    Order Matching Engine      |
+───────────────┬───────────────+
                │ (Publishes)
                ├──► [nte.orderbook.snapshots] (Protobuf)
                └──► [nte.trades.matched]      (Protobuf)
                        │
                        ▼
+───────────────────────────────+
|      Kafka Message Bus        |
+───────────────┬───────────────+
                │
        ┌───────┴───────────────────────┐
        │ (Consumes)                    │ (Consumes)
        ▼                               ▼
+───────────────────────+       +───────────────────────+
|  Order Book Consumer  |       |    Trade Consumer     |
+───────────┬───────────+       +───────────┬───────────+
            │                               │
            ├─► [Update Cache]              │
            │   (Redis Layer)               │
            ▼                               ▼
+───────────────────────────────────────────────────────+
|                     Client Manager                    |
|                (WebSocket Egress Server)              |
+───────────────────────────┬───────────────────────────+
                            │
                            │ (Broadcasts JSON)
                            ▼
              +───────────────────────────+
              |     External Clients      |
              +───────────────────────────+
```

*   **Ingestion Side**: Low-level event ingestion leverages [[decisions/decoupling-via-pub-sub]] to shield the core [[entities/order-matching-engine]] from downstream delays.
*   **Egress Side**: Protocol-converted, real-time JSON text is pushed to public connections via [[concepts/websocket-egress]], governed by the [[entities/client-manager]].

---

## 2. Path A: Level 2 Order Book Update Pipeline

The L2 order book pipeline handles the continuous propagation of depth updates to market participants, enabling them to reconstruct their local order books to make trading decisions.

### Step 1: Upstream Ingestion
The [[entities/order-matching-engine]] serializes current order book depths and publishes them to the Kafka event topic **[[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-orderbook-snapshots|nte.orderbook.snapshots]]**.
*   **Frequency**: Published at regular matching intervals or upon state-changing events.
*   **Format**: Structured using the `OrderBookSnapshot` definition in the [[entities/protobuf-schema]].

### Step 2: Ingestion & Deserialization
The **[[entities/order-book-consumer]]** operates within the `mdg-orderbook-group` consumer group.
*   It subscribes to [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-orderbook-snapshots|order-matching-engine (nte.orderbook.snapshots)]] and processes incoming messages:
```typescript
await this.consumer.subscribe({ topic: 'nte.orderbook.snapshots', fromBeginning: false });
```
*   **Deserialization Phase**: The raw Protobuf buffer is parsed into the internal domain model specified in `src/models/types.ts`. This translates binary fields to the `OrderBookEvent` and arrays of `PriceLevel` structures containing price, quantity, and order count.

### Step 3: State Cache Update
To facilitate rapid bootstrap times for newly-connected WebSocket clients, the deserialized L2 snapshot is immediately written to the **[[entities/redis-cache-layer]]**. 
*   This cache acts as the state-of-the-world database for the latest L2 depths.
*   Instead of waiting for the next cyclic update on the Kafka bus, a fresh client connection retrieves this cached L2 state to achieve instant initialization, as described in [[decisions/state-ingestion-recovery]].

### Step 4: Downstream Distribution
Once decoded, the consumer passes the snapshot to the `ClientManager` using the `broadcastOrderBook()` method.
```typescript
// Inside ClientManager.ts
broadcastOrderBook(snapshot: string) {
    for (const client of this.clients) {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify({ type: 'L2_UPDATE', data: snapshot }));
        }
    }
}
```
At this stage, the binary payload is translated into a standardized JSON packet (`L2_UPDATE`), adhering to the egress criteria established in [[decisions/protocol-translation]].

---

## 3. Path B: Real-Time Trade Tick Pipeline

The Trade Tick pipeline handles the lowest latency paths for actual order execution matches. Because trade events are discrete history points (rather than state snapshots), they bypass the caching layers and stream directly to client sockets.

### Step 1: Upstream Ingestion
Upon executing an order match, the [[entities/order-matching-engine]] instantly streams a trade confirmation to the Kafka topic **[[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-trades-matched|nte.trades.matched]]**.
*   **Format**: Encoded via [[entities/protobuf-schema]] using the `TradeTick` message type, representing execution price, quantity, execution timestamp (`matched_at_ns`), and the direction of the aggressive order (`taker_side`).

### Step 2: Consumption
The **[[entities/trade-consumer]]** operates within the `mdg-trade-group` Kafka consumer group.
*   It subscribes directly to the matches stream:
```typescript
await this.consumer.subscribe({ topic: 'nte.trades.matched', fromBeginning: false });
```
*   Because trades are immutable history ticks, the `TradeConsumer` bypasses the [[entities/redis-cache-layer]] entirely.

### Step 3: Distribution
The incoming trade event is handed off to `ClientManager.broadcastTrade()`. The manager maps the binary properties to the `TradeEvent` domain model (e.g., parsing nanosecond timestamps to high-precision representations) and broadcasts a serialized `TRADE_TICK` JSON payload.
```typescript
// Inside ClientManager.ts
broadcastTrade(trade: string) {
    for (const client of this.clients) {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify({ type: 'TRADE_TICK', data: trade }));
        }
    }
}
```

---

## 4. Key Cross-Cutting Pipeline Design Decisions

### Protocol Translation: Protobuf Ingress vs. JSON Egress
To balance internal backplane speed with external client accessibility, the data pipeline implements protocol conversion:
*   **Ingress**: Uses binary Google Protocol Buffers to minimize network overhead on internal brokers and reduce parsing costs.
*   **Egress**: Converts binary schemas to text-based JSON. This reduces integration friction for external trading desks, as detailed in [[decisions/protocol-translation]].

### Client Fairness and Iteration Latency
A primary mandate of the gateway is **simultaneity and fairness**: ensuring all subscribers receive market updates concurrently. 
*   **The Challenge**: In a sequential loop broadcasting over a `Set<WebSocket>`, clients located at the end of the set iteration can experience microsecond-level delays (head-of-line sweeping latency jitter) compared to clients at the front of the list.
*   **Design Trade-Off**: Currently, the [[entities/client-manager]] uses simple sequential iteration. For strict institutional trading fairness, future optimizations include group-based parallel socket writes and zero-copy kernel multicasting to neutralize this jitter, as documented in [[decisions/sequential-broadcast-fairness]].

### Pipeline Recovery and Hot Bootstrapping
When a client connects to [[concepts/websocket-egress]], waiting for the next cyclic L2 snapshot update from Kafka can result in a blank user interface or stale order books.
*   To resolve this, the **[[entities/redis-cache-layer]]** holds the warm-start order book state.
*   The connection lifecycle intercepts new WebSocket registrations, immediately queries Redis for the latest snapshot, and transmits it directly to the new socket before adding it to the active broadcast list. See [[decisions/state-ingestion-recovery]] for more details.