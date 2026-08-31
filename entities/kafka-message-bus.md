<!-- anchor: docker-compose.yml:L1-L100 sha:HEAD -->

# Kafka Message Bus

The **Kafka Message Bus** is the central, low-latency asynchronous transport layer of the Nexus Trading Exchange (NTE) platform. It serves as the primary mechanism for event streaming, decoupling core transactional matching components from public egress distribution networks. 

By utilizing Apache Kafka, the platform isolates the critical execution path of the matching engine from downstream client bandwidth, connections, and latency spikes.

---

## ## Responsibilities

The Kafka Message Bus coordinates the distribution of critical exchange events with the following responsibilities:

1. **Upstream Decoupling:** Insulates the [[entities/order-matching-engine]] from downstream processing pipelines, ensuring that slow WebSocket clients or analytical consumers do not impact order matching latency. See [[decisions/decoupling-via-pub-sub]] for the architectural rationale.
2. **Event Durability and Partitioning:** Guarantees that trade executions and order book state transitions are persisted, ordered within symbol partitions, and reproducible for recovery.
3. **High-Throughput Distribution:** Supports the high-frequency ingest requirements of both [[concepts/level-2-depth|Level 2 (L2) order book]] snapshots and real-time [[concepts/trade-tick-event|trade ticks]].
4. **Binary Protocol Transport:** Carries lightweight, binary-serialized payloads compliant with the defined [[entities/protobuf-schema]] to minimize internal transport serialization overhead and network footprint.

---

## ## Dependencies

The Kafka Message Bus integrates directly with several core upstream and downstream services:

* **Upstream Producer:** 
  * [[entities/order-matching-engine]] — Publishes real-time market updates to the primary trading topics.
* **Downstream Consumers:** 
  * [[entities/order-book-consumer]] — Consumes L2 snapshot messages from the [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-orderbook-snapshots|order-matching-engine (nte.orderbook.snapshots)]] topic.
  * [[entities/trade-consumer]] — Consumes matched trade executions from the [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-trades-matched|order-matching-engine (nte.trades.matched)]] topic.
* **Schemas & Wire Protocols:**
  * [[entities/protobuf-schema]] (`market_data.proto`) — Enforces the structured schema for binary serialization and deserialization across the message bus.

---

## Topic Configuration

The message bus is configured with dedicated, optimized topics designed to segment state snapshots from trade executions:

### 1. [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-orderbook-snapshots|order-matching-engine (nte.orderbook.snapshots)]]
* **Interface Reference:** [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-orderbook-snapshots|order-matching-engine (nte.orderbook.snapshots)]]
* **Payload Class:** `nte.marketdata.OrderBookSnapshot` (via [[entities/protobuf-schema]])
* **Purpose:** Distributes aggregated market depth updates at pre-configured intervals.
* **Characteristics:** High payload size variation (depends on active bids/asks), medium frequency. It is ingested by the [[entities/order-book-consumer]] to update the internal cache and broadcast the `L2_UPDATE` payload via [[concepts/websocket-egress]].
* **Downstream Impact:** Directly impacts the warm-start speed of new subscribers utilizing the [[entities/redis-cache-layer]] and [[decisions/state-ingestion-recovery]].

### 2. [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-trades-matched|order-matching-engine (nte.trades.matched)]]
* **Interface Reference:** [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-trades-matched|order-matching-engine (nte.trades.matched)]]
* **Payload Class:** `nte.marketdata.TradeTick` (via [[entities/protobuf-schema]])
* **Purpose:** Streams match-by-match trading executions.
* **Characteristics:** Small message sizes, extremely high frequency during high market activity. Configured for lowest-possible transport latency to ensure near-immediate delivery to the [[entities/trade-consumer]] and external client subscribers.

---

## Architecture Integration & Data Pipelines

The message bus acts as the boundary line between the **Internal High-Performance Domain** (binary protobuf, low-latency, private VPC) and the **Egress Distribution Domain** (JSON, WebSocket connection handling, public internet).

```
               INTERNAL VPC (Binary)                     │     EXTERNAL DMZ (Text)
                                                         │
  ┌──────────────────────────────┐                       │
  │   `Order Matching Engine`  │                       │
  └──────────────┬───────────────┘                       │
                 │ (Publishes)                           │
                 ▼                                       │
  ┌──────────────────────────────┐                       │
  │     `Kafka Message Bus`    │                       │
  │   - nte.orderbook.snapshots  │                       │
  │   - nte.trades.matched       │                       │
  └──────────────┬───────────────┘                       │
                 │                                       │
       ┌─────────┴─────────┐                             │
       │ (Consumes)        │ (Consumes)                  │
       ▼                   ▼                             │
┌──────────────┐    ┌──────────────┐                     │
│  `Order Book  │    │    [[Trade     │                     │
│  Consumer`  │    │  Consumer]]  │                     │
└──────┬───────┘    └──────┬───────┘                     │
       │                   │                             │
       │ (Internal Pass)   │                             │   `WebSocket Egress`
       ▼                   ▼                             │   - L2_UPDATE
┌──────────────────────────────────┐                     │   - TRADE_TICK
│         `Client Manager`       │─────────────────────┼─────────► [External Clients]
│       (WebSocket Server)         │                     │
└──────────────────────────────────┘                     │
```

This pipeline architecture represents the core of the [[concepts/data-flow-pipelines]], facilitating:
1. **Asynchronous Processing:** Stream isolation prevents the matching engine from waiting on client I/O or JSON serialization bottlenecks.
2. **Translation Decoupling:** Enables the system to absorb high-velocity binary events inside the VPC and carefully translate them into developer-friendly JSON format on the edge (as detailed in [[decisions/protocol-translation]]).
3. **Load Elasticity:** In the event of a sudden surge in consumer connections, the Kafka queue safely buffers the backlog of unprocessed events, maintaining the stability of the core matching engine.