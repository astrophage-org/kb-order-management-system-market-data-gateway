# WebSocket Egress: Real-Time Market Data Distribution

The WebSocket egress subsystem of the [[summaries/market-data-gateway]] serves as the terminal distribution layer of the Nexus Trading Exchange (NTE) platform. It is designed to broadcast real-time public market data—specifically [[concepts/level-2-depth|Level 2 (L2) order books]] and [[concepts/trade-tick-event|trade ticks]]—to external market participants with ultra-low latency and deterministic fairness.

By establishing a clear boundary between internal high-performance messaging protocols and external client connectivity, this egress layer implements the core architectural principle of **simultaneity and fairness** while shielding upstream components from the vagaries of internet network health.

---

## 1. Egress Architecture & Topology

The WebSocket egress layer sits downstream of the [[entities/kafka-message-bus]] and operates asynchronously to prevent slow clients from impacting internal system performance. 

```
                                  [[entities/kafka-message-bus]]
                                         │             │
                    (nte.orderbook.snapshots)          (nte.trades.matched)
                                         │             │
                                         ▼             ▼
                           [[entities/order-book-consumer]]  [[entities/trade-consumer]]
                                         │             │
                                         ▼             ▼
                                  ┌───────────────────────────┐
                                  │ [[entities/client-manager]]│◄─── [[entities/redis-cache-layer]]
                                  │    (WebSocket Server)     │     (Warm-start recovery)
                                  └──────────────┬────────────┘
                                                 │
                                 ┌───────────────┼───────────────┐
                                 ▼               ▼               ▼
                           [Client 1]      [Client 2]      [Client N]
                       (L2_UPDATE/JSON) (TRADE_TICK/JSON) (L2_UPDATE/JSON)
```

The data flow pipelines from ingestion to external broadcast are fully detailed in [[concepts/data-flow-pipelines]].

### Principal Actors

1.  **[[entities/client-manager|Client Manager]] (`ClientManager`):** The stateful WebSocket server (powered by the `ws` engine) running on port `8080`. It manages active connection state, tracks client lifecycles, and executes the broadcast loops.
2.  **[[entities/order-book-consumer|Order Book Consumer]] (`OrderBookConsumer`):** Consumes compiled L2 snapshots from the internal [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-orderbook-snapshots|order-matching-engine (nte.orderbook.snapshots)]] topic (see [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-orderbook-snapshots|order-matching-engine (nte.orderbook.snapshots)]]) and invokes `ClientManager.broadcastOrderBook()`.
3.  **[[entities/trade-consumer|Trade Consumer]] (`TradeConsumer`):** Consumes real-time trade matches from the internal [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-trades-matched|order-matching-engine (nte.trades.matched)]] topic (see [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-trades-matched|order-matching-engine (nte.trades.matched)]]) and invokes `ClientManager.broadcastTrade()`.

---

## 2. Protocol Translation: Binary to JSON Text

The gateway performs an essential conversion step between internal high-efficiency formats and standard external formats.

```
       [Internal Pipeline]                         [External Egress]
    Binary Protocol Buffers                     Human-Readable JSON
  (optimized for network throughput)         (optimized for client ease-of-use)
 ──────────────────────────────────► [MDG] ──────────────────────────────────►
     - OrderBookSnapshot                       - L2_UPDATE
     - TradeTick                               - TRADE_TICK
```

To support this translation, internal telemetry maps to public JSON payloads through a structured lifecycle:
1.  **Protobuf Ingress:** Internal events are received as binary structures defined in the [[entities/protobuf-schema]].
2.  **Internal Domain representation:** Decoded bytes are formatted into TypeScript models (`TradeEvent` and `OrderBookEvent`) defined in `src/models/types.ts`.
3.  **JSON External Egress:** The payloads are serialized to text JSON strings immediately prior to network transport to maximize developer accessibility, as detailed in [[decisions/protocol-translation]].

### Wire Formats (JSON Egress)

#### L2_UPDATE Message
```json
{
  "type": "L2_UPDATE",
  "data": {
    "symbol": "BTCUSD",
    "bids": [
      { "price": 64250.0, "quantity": 1.45, "orderCount": 3 },
      { "price": 64249.5, "quantity": 2.10, "orderCount": 5 }
    ],
    "asks": [
      { "price": 64251.0, "quantity": 0.85, "orderCount": 1 },
      { "price": 64252.0, "quantity": 3.42, "orderCount": 8 }
    ],
    "timestamp": 1711974000123
  }
}
```

#### TRADE_TICK Message
```json
{
  "type": "TRADE_TICK",
  "data": {
    "tradeId": "tx-98210398-a",
    "symbol": "BTCUSD",
    "price": 64250.0,
    "quantity": 0.15,
    "matchedAt": 1711974000123456,
    "takerSide": "BUY"
  }
}
```

---

## 3. Connection Lifecycle and Warm Bootstrapping

When a new external client connects to the WebSocket gateway, they must be rapidly synchronized to the current state of the market without waiting for next matching cycle.

```
[External Client]                 [Client Manager]            [[entities/redis-cache-layer]]
        │                                │                                │
        │─────── WSS Connection ────────►│                                │
        │                                │────── Get Latest Book State ──►│
        │                                │◄───── Return Cached Snapshot ──│
        │◄────── Send Init Snapshot ─────│                                │
        │                                │                                │
  (Client is now Warm-Started and waits for real-time Kafka broadcasts)
```

1.  **Connection Registration:** The `ClientManager` registers the connection in an in-memory `Set<WebSocket>` client pool.
2.  **State Ingestion Recovery:** Instead of relying on a slow database query or waiting for the next cyclic Kafka partition update, the gateway queries the [[entities/redis-cache-layer]].
3.  **Bootstrap Delivery:** The latest cached snapshot is dispatched immediately to the newly connected client, facilitating instant warm recovery as outlined in [[decisions/state-ingestion-recovery]].
4.  **Delta Synchronization:** Subsequent incremental broadcasts are pushed to the client via standard stream subscriptions.

---

## 4. Performance, Latency Fairness, and Jitter

A primary engineering challenge for public market data distribution is **fairness**: preventing any single client from receiving data significantly ahead of its peers. 

### Current Broadcast Implementation
The `ClientManager` iterates sequentially through its set of active connections to push serialized JSON packets:

```typescript
broadcastOrderBook(snapshot: string) {
    for (const client of this.clients) {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify({ type: 'L2_UPDATE', data: snapshot }));
        }
    }
}
```

### Sequential Jitter (Head-of-Line Sweeping Latency)
The loop above introduces linear latency degradation ($O(N)$), where the $N$-th client in the `Set` receives the book update later than the $1$-st client.
*   **Microsecond Jitter:** For hundreds of connections, the cumulative time taken to parse, allocate, and write TCP packets results in significant head-of-line sweeping latency.
*   **Architectural Trade-offs:** This simple implementation ensures memory safety and zero external dependencies but presents fairness trade-offs that are evaluated in [[decisions/sequential-broadcast-fairness]].

### Mitigation and Optimizations
To maintain institutional fairness, future releases plan to incorporate:
1.  **Serialized Cache Pre-Generation:** Moving `JSON.stringify()` out of the loop, ensuring serialization occurs only once per event.
2.  **Threaded Connection Pools:** Distributing socket descriptors across CPU-affinity-pinned worker threads.
3.  **Epoll-based Multicasting:** Utilizing low-level system calls to dispatch data to network interfaces in parallel batches.

---

## 5. Security & Gateway Isolation

By decoupling client distribution from internal event streams via the [[decisions/decoupling-via-pub-sub]] pattern, the gateway acts as a robust security and load boundary:
*   **Upstream Immunity:** Slow, unresponsive, or malicious external connections have their sockets terminated at the egress layer, preventing TCP backpressure from propagating up to the core [[entities/order-matching-engine]].
*   **IP Whitelisting & Connection Limits:** Connection thresholds per IP are enforced at the network edge to mitigate Distributed Denial of Service (DDoS) attacks.
*   **Message Filtering:** Only authorized, public trade ticks and aggregate L2 depths are exposed. Private order states and matching engine internals remain securely confined behind the [[entities/kafka-message-bus]].