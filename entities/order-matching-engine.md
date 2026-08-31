<!-- anchor: docs/architecture.md:L1-L100 sha:HEAD -->

# Order Matching Engine

The **Order Matching Engine (OME)** is the core transaction processing hub of the Nexus Trading Exchange (NTE) platform. It operates as an ultra-low latency execution system that manages the Central Limit Order Book (CLOB), matches incoming orders according to price-time priority, and generates raw state updates. 

Within the [[index|Market Data Gateway (MDG) Architecture]], the OME serves as the upstream publisher of record, feeding raw execution data and structural depth updates directly to the [[entities/kafka-message-bus]] for downstream consumption.

---

## ## Responsibilities

*   **Order Matching Execution:** Executes order-matching logic based on a strict price-time priority (FIFO) algorithm.
*   **Level 2 Depth Computation:** Computes and tracks aggregate order book depth updates across all symbols, generating [[concepts/level-2-depth|Level 2 (L2) order book]] snapshots.
*   **Trade Event Generation:** Emits execution records representing executed matches as [[concepts/trade-tick-event|trade tick events]].
*   **High-Throughput Upstream Publishing:** Serializes state changes and broadcasts them instantly to the [[entities/kafka-message-bus]] to prevent internal engine backpressure, utilizing the principles detailed in [[decisions/decoupling-via-pub-sub]].

---

## ## Dependencies

The OME acts as the source-of-truth upstream publisher for several downstream MDG systems:

*   **Internal Event Streaming (Outbound):**
    *   [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-orderbook-snapshots|nte.orderbook.snapshots]] (Topic): Target for L2 snapshot broadcasts.
    *   [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-trades-matched|nte.trades.matched]] (Topic): Target for real-time match execution events.
*   **Downstream Consumers:**
    *   [[entities/order-book-consumer]]: Ingests the L2 order book snapshots.
    *   [[entities/trade-consumer]]: Ingests real-time trade match events.
*   **Data Contracts:** 
    *   [[entities/protobuf-schema]]: Uses `market_data.proto` (`OrderBookSnapshot` and `TradeTick` messages) to serialize payloads prior to Kafka ingress.

---

## Core Matching Logic & State Generation

The core execution loop of the OME is completely isolated from network and disk I/O to guarantee microsecond-level determinism. 

```
 [Limit / Market Order] ──► [Matching Loop (FIFO)]
                                 │
                   ┌─────────────┴─────────────┐
                   ▼                           ▼
            [Matched Trades]           [Depth Updates]
                   │                           │
                   ▼                           ▼
            (TradeTick Proto)         (OrderBookSnapshot Proto)
                   │                           │
                   ▼                           ▼
             [nte.trades.matched]     [nte.orderbook.snapshots]
```

### 1. Match Engine Loop (FIFO)
Orders are processed sequentially. When an incoming order matches existing resting liquidity:
1.  A trade is executed, immediately generating a `TradeTick` payload.
2.  The resting order’s remaining quantity is updated, or it is removed from the book.
3.  The bid/ask levels are recalculated to produce a new [[concepts/level-2-depth|L2 book snapshot]].

### 2. Upstream Protocol Serialization
To maximize network throughput and minimize encoding latency within the matching path, the OME serializes events using binary **Protocol Buffers** before broadcasting them to Kafka (see [[decisions/protocol-translation]]). 

*   **Order Book Snapshots:** Transmitted as `OrderBookSnapshot` payloads, which pack price arrays (`Level`) for both bids and asks into an optimized binary buffer.
*   **Trade Ticks:** Transmitted as `TradeTick` payloads, capturing unique trade IDs, quantities, matching side, and nanosecond-precision execution timestamps.

---

## Upstream Event Publishing

The OME publishes events asynchronously to the [[entities/kafka-message-bus]]. This design ensures that slow WebSocket clients downstream do not impact the core matching engine's throughput.

### Outbound Event Mapping

| Source Event | Protobuf Message | Target Kafka Topic | Downstream Consumer |
| :--- | :--- | :--- | :--- |
| **L2 Depth State** | `OrderBookSnapshot` | [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-orderbook-snapshots|order-matching-engine (nte.orderbook.snapshots)]] | [[entities/order-book-consumer]] |
| **Trade Match** | `TradeTick` | [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-trades-matched|order-matching-engine (nte.trades.matched)]] | [[entities/trade-consumer]] |

### Partitioning & Message Flow
The OME partitions messages by currency pair symbol (e.g., `BTCUSD`, `ETHUSD`). This ensures that:
1.  All updates for a specific market are processed sequentially on the same Kafka partition.
2.  Downstream consumers can scale horizontally by partition while still maintaining a strict chronological order of price updates per symbol.
3.  The MDG can guarantee simultaneous broadcast fairness (as detailed in [[decisions/sequential-broadcast-fairness]]) by keeping downstream pipelines highly aligned with these partitioned streams.