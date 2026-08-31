<!-- anchor: README.md:L1-L100 sha:HEAD -->

# Market Data Gateway (MDG): Architecture and Data Distribution

The **Market Data Gateway (MDG)** is a high-performance, low-latency egress subsystem of the Nexus Trading Exchange (NTE) platform. It is responsible for distributing real-time public market data—specifically [[concepts/level-2-depth|Level 2 (L2) order books]] and [[concepts/trade-tick-event|trade ticks]]—to external clients. 

The primary architectural mandate of the MDG is **simultaneity and fairness**: ensuring that all connected external subscribers receive market updates concurrently, minimizing latency jitter across client connections as detailed in [[decisions/sequential-broadcast-fairness]].

For a high-level overview of how the MDG fits into the global exchange topology, please refer to the [[index]].

---

## 1. System Architecture & Components

The MDG is designed as a decoupled, event-driven gateway that insulates the core matching logic from downstream internet-facing distribution concerns. This isolation is further documented in [[decisions/decoupling-via-pub-sub]].

```
       ┌──────────────────────────────┐
       │   [[entities/order-matching-engine]]  │
       └──────────────┬───────────────┘
                      │ (Publishes)
                      ▼
       ┌──────────────────────────────┐
       │    [[entities/kafka-message-bus]]   │
       │   - nte.orderbook.snapshots  │
       │   - nte.trades.matched       │
       └──────────────┬───────────────┘
                      │
            ┌─────────┴─────────┐
            │ (Consumes)        │ (Consumes)
            ▼                   ▼
     ┌──────────────┐    ┌──────────────┐
     │ [[entities/order-book-consumer]]│    │  [[entities/trade-consumer]]  │
     └──────┬───────┘    └──────┬───────┘
            │                   │
            │ (Internal Pass)   │
            ▼                   ▼
     ┌──────────────────────────────────┐
     │        [[entities/client-manager]]       │◄─────── [[entities/redis-cache-layer]]
     │       (WebSocket Server)         │  (L2 State Recovery - Planned)
     └────────────────┬─────────────────┘
                      │
            ┌─────────┼─────────┐ (Broadcasts JSON)
            ▼         ▼         ▼
         [Client]  [Client]  [Client]
```

### Core Components

*   **[[entities/client-manager]] (`ClientManager`):** A stateful WebSocket server (running on port `8080` by default) implemented using the `ws` library. It maintains the registry of active external client connections (`Set<WebSocket>`) and provides high-efficiency broadcast APIs to push serialized payloads to all connected clients.
*   **[[entities/order-book-consumer]] (`OrderBookConsumer`):** A dedicated worker running in the `mdg-orderbook-group` Kafka consumer group. It subscribes to the [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-orderbook-snapshots|order-matching-engine (nte.orderbook.snapshots)]] topic and feeds updates directly to the [[entities/client-manager]].
*   **[[entities/trade-consumer]] (`TradeConsumer`):** A dedicated worker running in the `mdg-trade-group` Kafka consumer group. It subscribes to the [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-trades-matched|order-matching-engine (nte.trades.matched)]] topic to stream real-time match events to the [[entities/client-manager]].
*   **[[entities/protobuf-schema]] (`market_data.proto`):** The cross-service data contract defining binary payloads for `OrderBookSnapshot` and `TradeTick` shared between the internal trading core and the MDG.
*   **[[entities/redis-cache-layer]]:** A high-performance, fast-recovery cache deployed adjacent to the gateway to store warm-start order books, facilitating instant recovery as defined in [[decisions/state-ingestion-recovery]].

---

## 2. End-to-End Data Flow

The MDG orchestrates two primary, asynchronous unidirectional pipelines, details of which can be found in [[concepts/data-flow-pipelines]]:

### Pipeline A: Level 2 Order Book Updates
1.  **Ingestion:** The [[entities/order-matching-engine]] publishes L2 book snapshots to the Kafka topic [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-orderbook-snapshots|order-matching-engine (nte.orderbook.snapshots)]].
2.  **Consumption:** The [[entities/order-book-consumer]] polls this topic.
3.  **Parsing/Deserialization:** The binary Protobuf schema defined in `market_data.proto` is parsed (marked as a TODO optimization in code) to construct the internal update model.
4.  **Distribution:** The update is handed off to `ClientManager.broadcastOrderBook()`, which packages the payload as an `L2_UPDATE` and broadcasts it to all connected subscribers via [[concepts/websocket-egress]].

### Pipeline B: Trade Executions (Trade Ticks)
1.  **Ingestion:** The [[entities/order-matching-engine]] serializes execution events to [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-trades-matched|order-matching-engine (nte.trades.matched)]] upon matching orders.
2.  **Consumption:** The [[entities/trade-consumer]] reads the matched trades instantly from the Kafka partition.
3.  **Distribution:** The consumer hands the payload to `ClientManager.broadcastTrade()`, which wraps the trade in a `TRADE_TICK` structure and writes it directly to open WebSocket channels.

---

## 3. Key Abstractions & Data Models

The system architecture maintains a clean separation between raw wire protocols and internal domain representations:

```
                  [Protobuf Ingress]
            (protos/market_data.proto)
             - OrderBookSnapshot
             - TradeTick
                      │
                      ▼
               [Domain Models] (src/models/types.ts)
                - PriceLevel
                - OrderBookEvent
                - TradeEvent
                      │
                      ▼
              [JSON Client Egress]
               - L2_UPDATE
               - TRADE_TICK
```

### Domain Models (`src/models/types.ts`)
*   **`PriceLevel`**: Represents aggregated depth at a given price boundary.
    ```typescript
    interface PriceLevel {
        price: number;
        quantity: number;
        orderCount: number;
    }
    ```
*   **`OrderBookEvent`**: High-performance payload representing bids and asks arrays alongside an epoch timestamp.
*   **`TradeEvent`**: Outlines a matched execution with a `takerSide` tag (`BUY` or `SELL`), utilizing nanosecond-precision execution timestamps.

---

## 4. Design Decisions & Trade-offs

### 1. Decoupling via Pub-Sub
By leveraging the [[entities/kafka-message-bus]] as an intermediary broker, the critical path of the [[entities/order-matching-engine]] is isolated from network fluctuations, slow consumer rates, or spikes in external client connections. This design pattern is detailed in [[decisions/decoupling-via-pub-sub]].

### 2. Protocol Translation: Binary Ingress vs. Text Egress
As discussed in [[decisions/protocol-translation]]:
*   **Ingress (Internal):** Uses [[entities/protobuf-schema|Protocol Buffers]] for extremely compact network footprints and minimal serialization latency across the internal corporate network.
*   **Egress (External):** Translates data models to JSON string payloads for [[concepts/websocket-egress]]. This incurs a small serialization cost but maximizes external developer accessibility and client-side compatibility.

### 3. State Ingestion & Fast Recovery
As defined in [[decisions/state-ingestion-recovery]], a [[entities/redis-cache-layer]] runs adjacent to the gateway:
*   **State Recovery:** The [[entities/redis-cache-layer]] is designed to cache the latest state of the L2 order book. When a new client establishes a WebSocket connection, they immediately fetch a warm-start snapshot from Redis instead of waiting for the next cyclic broadcast over Kafka.

### 4. Sequential Broadcast vs. Latency Fairness
As explored in [[decisions/sequential-broadcast-fairness]], the [[entities/client-manager]] broadcasts events using a sequential loop over its active socket pool:
```typescript
for (const client of this.clients) {
    if (client.readyState === WebSocket.OPEN) {
        client.send(JSON.stringify({ type: 'L2_UPDATE', data: snapshot }));
    }
}
```
*   **Trade-off:** While simple and free of allocation overhead, sequential iterations can introduce microsecond-level discrepancies between the first and last clients in the array (head-of-line sweeping latency). For strict institutional fairness, this is a vector for future optimizations, such as parallel socket group dispatches or zero-copy Kernel-level multicasting.