# Architecture: Market Data Gateway (MDG)

The **Market Data Gateway (MDG)** is a high-performance, low-latency egress subsystem of the Nexus Trading Exchange (NTE) platform. It is responsible for distributing real-time public market data—specifically `Level 2 (L2) order books` and `trade ticks`—to external clients. 

The primary architectural mandate of the MDG is **simultaneity and fairness**: ensuring that all connected external subscribers receive market updates concurrently, minimizing latency jitter across client connections.

---

## 1. System Architecture & Components

The MDG is designed as a decoupled, event-driven gateway that insulates the core matching logic from downstream internet-facing distribution concerns.

```
       ┌──────────────────────────────┐
       │    `Order Matching Engine`   │
       └──────────────┬───────────────┘
                      │ (Publishes)
                      ▼
       ┌──────────────────────────────┐
       │     `Kafka Message Bus`    │
       │   - nte.orderbook.snapshots  │
       │   - nte.trades.matched       │
       └──────────────┬───────────────┘
                      │
            ┌─────────┴─────────┐
            │ (Consumes)        │ (Consumes)
            ▼                   ▼
     ┌──────────────┐    ┌──────────────┐
     │  `Order Book  │    │    [[Trade     │
     │  Consumer`  │    │  Consumer]]  │
     └──────┬───────┘    └──────┬───────┘
            │                   │
            │ (Internal Pass)   │
            ▼                   ▼
     ┌──────────────────────────────────┐
     │         `Client Manager`       │◄─────── `Redis Cache Layer`
     │       (WebSocket Server)         │  (L2 State Recovery - Planned)
     └────────────────┬─────────────────┘
                      │
            ┌─────────┼─────────┐ (Broadcasts JSON)
            ▼         ▼         ▼
         [Client]  [Client]  [Client]
```

### Main Components

*   **`Client Manager` (`ClientManager`):** A stateful WebSocket server implemented using the `ws` library. It maintains the registry of active external client connections (`Set<WebSocket>`) and provides high-efficiency broadcast APIs to push serialized payloads to all connected clients.
*   **`Order Book Consumer` (`OrderBookConsumer`):** A dedicated worker running in the `mdg-orderbook-group` Kafka consumer group. It subscribes to the [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-orderbook-snapshots|order-matching-engine (nte.orderbook.snapshots)]] topic and feeds updates directly to the `Client Manager`.
*   **`Trade Consumer` (`TradeConsumer`):** A dedicated worker in the `mdg-trade-group` Kafka consumer group. It subscribes to the [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-trades-matched|order-matching-engine (nte.trades.matched)]] topic to stream real-time match events to the `Client Manager`.
*   **`Protocol Buffers Schema` (`market_data.proto`):** The cross-service data contract defining binary payloads for `OrderBookSnapshot` and `TradeTick` shared between the internal trading core and the MDG.

---

## 2. End-to-End Data Flow

The MDG orchestrates two primary, asynchronous unidirectional pipelines:

### Path A: Level 2 Order Book Updates
1.  **Ingestion:** The `Order Matching Engine` publishes L2 book snapshots to the Kafka topic [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-orderbook-snapshots|order-matching-engine (nte.orderbook.snapshots)]].
2.  **Consumption:** The `Order Book Consumer` polls this topic.
3.  **Parsing/Deserialization:** The binary Protobuf schema defined in `market_data.proto` is parsed (marked as a TODO optimization in code) to construct the internal update model.
4.  **Distribution:** The update is handed off to `ClientManager.broadcastOrderBook()`, which packages the payload as an `L2_UPDATE` and broadcasts it to all connected subscribers via `WebSocket Egress`.

### Path B: Trade Executions (Trade Ticks)
1.  **Ingestion:** The `Order Matching Engine` serializes execution events to [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-trades-matched|order-matching-engine (nte.trades.matched)]] upon matching orders.
2.  **Consumption:** The `Trade Consumer` reads the matched trades instantly from the Kafka partition.
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
By leveraging the `Kafka Message Bus` as an intermediary broker, the critical path of the `Order Matching Engine` is isolated from network fluctuations, slow consumer rates, or spikes in external client connections. 

### 2. Protocol Translation: Binary Ingress vs. Text Egress
*   **Ingress (Internal):** Uses `Protocol Buffers` for extremely compact network footprints and minimal serialization latency across the internal corporate network.
*   **Egress (External):** Translates data models to JSON string payloads for `WebSocket Egress`. This incurs a small serialization cost but maximizes external developer accessibility and client-side compatibility.

### 3. State Ingestion & Fast Recovery
As defined in the system topology, a `Redis Cache Layer` runs adjacent to the gateway.
*   **State Recovery:** The `Redis Cache Layer` is designed to cache the latest state of the L2 order book. When a new client establishes a WebSocket connection, they immediately fetch a warm-start snapshot from Redis instead of waiting for the next cyclic broadcast over Kafka.

### 4. Sequential Broadcast vs. Latency Fairness
The `Client Manager` broadcasts events using a sequential loop over its active socket pool:
```typescript
for (const client of this.clients) {
    if (client.readyState === WebSocket.OPEN) {
        client.send(JSON.stringify({ type: 'L2_UPDATE', data: snapshot }));
    }
}
```
*   **Trade-off:** While simple and free of allocation overhead, sequential iterations can introduce microsecond-level discrepancies between the first and last clients in the array (head-of-line sweeping latency). For strict institutional fairness, this is a vector for future optimizations, such as parallel socket group dispatches or zero-copy Kernel-level multicasting.