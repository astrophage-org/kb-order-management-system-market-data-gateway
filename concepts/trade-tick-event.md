# Trade Tick Event Execution Data Model

In high-frequency electronic trading, a **Trade Tick Event** represents a completed trade execution resulting from a successful match between two opposing orders (a bid and an ask). 

Within the **Nexus Trading Exchange (NTE)**, trade ticks are published instantly by the upstream [[entities/order-matching-engine]] upon matching incoming orders against resting liquidity. These ticks serve as the definitive real-time history of public market executions, driving charts, tape displays, volume metrics, and downstream risk/compliance systems.

---

## 1. End-to-End Trade Tick Pipeline

The lifecycle of a trade tick spans multiple microservices and transport formats to maintain high performance and low-latency distribution. This data flow is detailed in [[concepts/data-flow-pipelines]].

```
┌────────────────────────────────────────────────────────┐
│               [[entities/order-matching-engine]]       │
│  - Executes aggressive order against resting passive  │
└───────────────────────────┬────────────────────────────┘
                            │ (Publishes Binary Protobuf)
                            ▼
┌────────────────────────────────────────────────────────┐
│           [[entities/kafka-message-bus]]               │
│  - Topic: nte.trades.matched                           │
└───────────────────────────┬────────────────────────────┘
                            │
                            │ (Polled by Consumer Group)
                            ▼
┌────────────────────────────────────────────────────────┐
│               [[entities/trade-consumer]]              │
│  - Real-time Kafka consumer (mdg-trade-group)          │
└───────────────────────────┬────────────────────────────┘
                            │
                            │ (Pass-through / Translation)
                            ▼
┌────────────────────────────────────────────────────────┐
│               [[entities/client-manager]]              │
│  - WebSocket Server (Active Subscriber Connection Pool) │
└───────────────────────────┬────────────────────────────┘
                            │ (Broadcasts JSON)
                            ▼
┌────────────────────────────────────────────────────────┐
│               [[concepts/websocket-egress]]            │
│  - Sequential dispatch to external client connections  │
└────────────────────────────────────────────────────────┘
```

1. **Generation**: The [[entities/order-matching-engine]] executes an aggressive order against a passive limit order. It emits a binary representation of the execution to the Kafka topic [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-trades-matched|order-matching-engine (nte.trades.matched)]].
2. **Ingress Streaming**: The [[entities/kafka-message-bus]] routes the event to the [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-trades-matched|order-matching-engine (nte.trades.matched)]] topic.
3. **Gateway Ingestion**: The [[entities/trade-consumer]] polls the topic within the `mdg-trade-group` consumer group.
4. **Data Translation**: The binary Protobuf format is translated into an easy-to-use JSON text representation as part of the [[decisions/protocol-translation]] architecture.
5. **Egress Broadcast**: The [[entities/client-manager]] broadcasts the structured JSON payload via [[concepts/websocket-egress]] to all connected clients.

---

## 2. Schema Definitions

To optimize both internal transmission and public accessibility, trade ticks are represented in three distinct models as they traverse the system.

### A. Ingress Protocol Buffer Schema (`market_data.proto`)
The internal network representation uses [[entities/protobuf-schema]] on top of the [[entities/kafka-message-bus]] to minimize payload size and serialization time.

```protobuf
syntax = "proto3";
package nte.marketdata;

message TradeTick {
    string trade_id = 1;
    string symbol = 2;
    double price = 3;
    double quantity = 4;
    int64 matched_at_ns = 5;
    string taker_side = 6; // "BUY" or "SELL"
}
```

### B. Gateway Domain Model (`src/models/types.ts`)
The internal TypeScript interface used by the Market Data Gateway codebase to manipulate incoming tick events.

```typescript
export interface TradeEvent {
    tradeId: string;
    symbol: string;
    price: number;
    quantity: number;
    matchedAt: number; // Milliseconds or nanoseconds epoch
    takerSide: 'BUY' | 'SELL';
}
```

### C. Public WebSocket Egress JSON Schema
External subscribers receive trade tick messages wrapped in a standardized transport envelope to facilitate deserialization on client applications.

```json
{
  "type": "TRADE_TICK",
  "data": {
    "trade_id": "9a1f4b82-623c-4903-8bfb-887e0766be12",
    "symbol": "BTC-USD",
    "price": 64250.50,
    "quantity": 0.145,
    "matched_at_ns": 1698183145000123456,
    "taker_side": "BUY"
  }
}
```

---

## 3. Key Execution Attributes

### Nanosecond Precision Timestamping (`matched_at_ns`)
Financial systems depend on microsecond-level accuracy to preserve sequence order, build chronological order books, and resolve disputes. The `matched_at_ns` field records the exact epoch nanosecond time at which the engine logic executed the matching transaction. This allows:
* **High-Frequency Trading (HFT) Auditing**: Precise calculations of queue position, latency profile, and sweep patterns.
* **Deterministic Sequencing**: Resolving correct ordering when trades occur in sub-millisecond windows.

### Taker Side Aggression Identifier (`taker_side`)
The `taker_side` (either `BUY` or `SELL`) represents the side of the incoming **aggressive** (crossing) order that triggered the execution against the passive resting order in the book. 
* **Taker Side = `BUY`**: An aggressive buyer matched against a resting passive sell limit order. This is interpreted by charting platforms as a transaction executing at the **Ask** (often color-coded green).
* **Taker Side = `SELL`**: An aggressive seller matched against a resting passive buy limit order. This is interpreted as a transaction executing at the **Bid** (often color-coded red).

By calculating the proportion of BUY vs. SELL taker trades, client applications can compute real-time order flow toxicity and buy/sell pressure indicators without having to reconstruct the [[concepts/level-2-depth]] order book history.

---

## 4. Architectural Trade-offs & Decisions

* **Asynchronous Boundary via Pub-Sub**: The [[entities/order-matching-engine]] does not block on external client delivery. By delegating the execution logs to the [[entities/kafka-message-bus]], matching operations remain isolated from slower internet-facing connections (see [[decisions/decoupling-via-pub-sub]]).
* **Protocol Translation**: Translating high-performance binary Protobuf streams into standard JSON over WebSockets compromises a small amount of gateway CPU performance in favor of developer integration speed and web client capabilities (see [[decisions/protocol-translation]]).
* **Broadcast Fairness**: The trade tick distribution logic currently iterates sequentially over a collection of client connections within the [[entities/client-manager]]. For high-frequency use cases, this introduces microsecond-level jitter, which is subject to planned performance enhancements (see [[decisions/sequential-broadcast-fairness]]).