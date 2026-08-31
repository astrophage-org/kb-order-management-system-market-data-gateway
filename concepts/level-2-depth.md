# Level 2 (L2) Depth Representation

In modern high-frequency trading platforms, market visibility is categorized by depth. While Level 1 (L1) data only displays the best bid and ask (the top of the book), **Level 2 (L2) Depth** provides an aggregated view of the order book, containing multiple price levels of buy (bids) and sell (asks) orders. This representation is critical for market participants to evaluate liquidity density, spot imbalances, and model transaction costs.

In the **Market Data Gateway (MDG)**, the L2 order book pipeline processes aggregated snapshot events produced by the upstream [[entities/order-matching-engine]] and translates them into real-time market updates for external clients.

---

## 1. The L2 Data Model

An L2 order book representation is structured as two sorted arrays of aggregated price points, maintaining bids in descending order (highest price first) and asks in ascending order (lowest price first).

### Domain Model Structure (`src/models/types.ts`)

In the MDG runtime, individual levels and order book events are represented using highly structured TypeScript models:

```typescript
export interface PriceLevel {
    price: number;       // Aggregated limit price boundary
    quantity: number;    // Cumulative volume available at this price
    orderCount: number;  // Number of active limit orders at this price
}

export interface OrderBookEvent {
    symbol: string;      // Financial instrument identifier (e.g., BTCUSD)
    bids: PriceLevel[];  // Sorted descending array of buy liquidity
    asks: PriceLevel[];  // Sorted ascending array of sell liquidity
    timestamp: number;   // Epoch timestamp (millisecond precision for external delivery)
}
```

---

## 2. Inbound Serialization: Binary Protobuf Schema

To maximize high-throughput streaming over the [[entities/kafka-message-bus]], the [[entities/order-matching-engine]] packages order book snapshots using a compressed binary protocol. 

The MDG ingests these events from the [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-orderbook-snapshots|order-matching-engine (nte.orderbook.snapshots)]] topic (documented on the external [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-orderbook-snapshots|order-matching-engine topic list]]) using the binary structure defined in the global [[entities/protobuf-schema]]:

```protobuf
syntax = "proto3";
package nte.marketdata;

message OrderBookSnapshot {
    string symbol = 1;
    repeated Level bids = 2;
    repeated Level asks = 3;
    int64 timestamp_ns = 4;
}

message Level {
    double price = 1;
    double quantity = 2;
    int32 order_count = 3;
}
```

### Inbound Processing Sequence
```
┌─────────────────────────────────┐
│     Order Matching Engine       │
└────────────────┬────────────────┘
                 │ (Produces Proto Binary)
                 ▼
┌─────────────────────────────────┐
│  Kafka: nte.orderbook.snapshots │
└────────────────┬────────────────┘
                 │ (Consumed by)
                 ▼
┌─────────────────────────────────┐
│       OrderBookConsumer         │
└────────────────┬────────────────┘
                 │ (Deserializes Protobuf -> Map to Domain Model)
                 ▼
┌─────────────────────────────────┐
│         ClientManager           │
└─────────────────────────────────┘
```

The [[entities/order-book-consumer]] handles the real-time polling of this topic, preparing the incoming payload for deserialization and handover to the [[entities/client-manager]].

---

## 3. Outbound Translation: JSON Text Egress

While the internal services achieve ultra-low latency footprint via Protobuf, external API clients generally require high accessibility and simpler parsing logic. 

As detailed in the [[decisions/protocol-translation]] architectural decision record, the MDG converts binary Protobuf data into JSON formats over WebSocket connections. 

### WebSocket Egress Schema
When the `ClientManager.broadcastOrderBook()` method is triggered, it envelopes the raw depth into an `L2_UPDATE` structure:

```json
{
  "type": "L2_UPDATE",
  "data": {
    "symbol": "BTCUSD",
    "bids": [
      { "price": 95100.5, "quantity": 1.25, "orderCount": 3 },
      { "price": 95099.0, "quantity": 4.81, "orderCount": 11 }
    ],
    "asks": [
      { "price": 95101.0, "quantity": 0.85, "orderCount": 2 },
      { "price": 95102.5, "quantity": 5.12, "orderCount": 7 }
    ],
    "timestamp_ns": 1718029512000000000
  }
}
```

This translation pipeline ensures that external trading algorithms can consume real-time price depth instantly, mapping to local order book representations without incurring a complex dependencies chain.

---

## 4. Ingestion Lifecycle & Client State Recovery

The management of L2 depth events presents a specific challenge: **State Synchronization for New Clients**. Because L2 snapshots are published as discrete packets over Kafka, a client joining mid-cycle might experience a blind spot before the next periodic update is emitted.

To bypass this latency gap, the platform utilizes a split ingestion path:

```
                  ┌──────────────────────────────┐
                  │    Order Matching Engine     │
                  └──────────────┬───────────────┘
                                 │
                                 ▼
                  ┌──────────────────────────────┐
                  │     Kafka Message Bus        │
                  │   nte.orderbook.snapshots    │
                  └──────────────┬───────────────┘
                                 │
                        ┌────────┴────────┐
                        ▼                 ▼
             ┌────────────────────┐     ┌────────────────────┐
             │ Order Book Consumer│     │ Redis Cache Layer  │
             └──────────┬─────────┘     └─────────┬──────────┘
                        │                         │ (Direct Read
                        │                         │  for Cold Starts)
                        ▼                         ▼
             ┌───────────────────────────────────────────────┐
             │                 Client Manager                │
             │               (WebSocket Egress)              │
             └──────────────────────┬────────────────────────┘
                                    │ (Broadcast JSON)
                                    ▼
                             [Active Clients]
```

1. **Continuous Pipeline**: The [[entities/order-book-consumer]] processes the steady stream of updates over the [[concepts/data-flow-pipelines]] and feeds them to the [[entities/client-manager]] for fast, multi-client broadcast via [[concepts/websocket-egress]].
2. **Warm-Start Recovery**: Simultaneously, the [[entities/redis-cache-layer]] acts as a fast-recovery state cache. Upon connection establishment, a new client receives a direct warm-start pull from Redis rather than waiting for the next cyclic Kafka snapshot. This critical recovery pipeline is specified under [[decisions/state-ingestion-recovery]].

---

## 5. Performance Considerations

L2 order book streams represent the highest volume of data emitted by the exchange. In times of high market volatility, thousands of updates can occur per second. The MDG maintains performance viability via several design structures:

* **Decoupling from the Core Matching Engine**: Thanks to [[decisions/decoupling-via-pub-sub]], network fluctuations at the client boundary cannot block the central order matching cycle.
* **Low Allocation Iteration**: The [[entities/client-manager]] uses an array of open sockets to broadcast updates instantly. For insights on handling microsecond jitters during multi-client broadcasts, see [[decisions/sequential-broadcast-fairness]].
* **L2 Depth Limiting**: In production configurations, the matching engine truncates L2 books to a specific depth (e.g., top 20 or top 50 levels) before publishing. This guarantees a predictable network footprint and prevents massive arrays from causing head-of-line blocking on client connections.

## See Also
* [[concepts/trade-tick-event]] — Real-time execution events processed alongside L2 updates.
* [[concepts/data-flow-pipelines]] — Complete telemetry pipeline mapping from matching engine to client.
* [[entities/order-book-consumer]] — The background Kafka worker processing these depth updates.