# Architectural Decision Record: Asynchronous Decoupling of the Core Matching Engine

## Status

**Accepted**

## Context

The Nexus Trading Exchange (NTE) platform demands ultra-low, highly deterministic execution latency inside its core processing loop. The [[entities/order-matching-engine]] (OME) must remain completely insulated from external client network conditions, slow connection speeds, connection churn, and public internet-facing bottlenecks. 

If external WebSocket clients subscribed directly to the OME or if the OME synchronous execution loop were blocked by slow client write operations, the entire exchange would suffer from catastrophic head-of-line blocking. Furthermore, spikes in public connection counts could starve matching threads of system resources (CPU, file descriptors, memory).

To distribute public market data—specifically [[concepts/level-2-depth|Level 2 (L2) order books]] and [[concepts/trade-tick-event|trade ticks]]—the system architecture must guarantee that:
1. The [[entities/order-matching-engine]] remains completely unaware of downstream client connections.
2. Market data events are distributed downstream asynchronously.
3. Downstream consumer lag or network congestion does not propagate backpressure to the OME.
4. Clients receive synchronized updates concurrently via [[concepts/websocket-egress]], maintaining fairness.

For more details on the end-to-end telemetry architecture, refer to the [[index]] and [[summaries/market-data-gateway]] pages.

## Decision

We have decided to architecturally isolate the core [[entities/order-matching-engine]] from external clients by introducing an asynchronous, pub-sub messaging layer using the [[entities/kafka-message-bus]].

```
┌─────────────────────────────────┐
│ [[entities/order-matching-engine]] │
└────────────────┬────────────────┘
                 │ (Asynchronous, Protobuf Serialization)
                 ▼
┌─────────────────────────────────┐
│   [[entities/kafka-message-bus]]   │
│ - [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-orderbook-snapshots|nte.orderbook.snapshots]] │
│ - [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-trades-matched|nte.trades.matched]]       │
└────────────────┬────────────────┘
                 │
      ┌──────────┴──────────┐
      │ (Pull-based)        │ (Pull-based)
      ▼                     ▼
┌──────────────┐     ┌──────────────┐
│  [[entities/order-book-consumer]]  │     │   [[entities/trade-consumer]]    │
└──────┬───────┘     └──────┬───────┘
       │                    │
       └─────────┬──────────┘
                 │ (Internal Handoff)
                 ▼
       ┌────────────────────┐
       │ [[entities/client-manager]] │ ◄── [Synchronous Snapshot Warm-Start] ── [[entities/redis-cache-layer]]
       └─────────┬──────────┘
                 │
                 ▼
        [WebSocket Clients]
```

### Key Technical Aspects

1. **Unidirectional Pub-Sub Topics**: 
   The OME acts purely as a publisher to the [[entities/kafka-message-bus]], using optimized binary protocols defined in [[entities/protobuf-schema]]. It pushes data directly to dedicated Kafka topics:
   * **[[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-orderbook-snapshots|order-matching-engine (nte.orderbook.snapshots)]]**: Distributes L2 order book updates.
   * **[[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-trades-matched|order-matching-engine (nte.trades.matched)]]**: Distributes real-time trade match events.

2. **Decoupled Consumer Layer**:
   We will deploy highly specialized, independent Kafka consumer groups inside the Market Data Gateway (MDG):
   * **[[entities/order-book-consumer]]**: Running inside the dedicated consumer group `mdg-orderbook-group` to process incoming L2 snapshot updates.
   * **[[entities/trade-consumer]]**: Running inside the consumer group `mdg-trade-group` to handle real-time matched executions.

3. **Stateful Connection Registry via Client Manager**:
   The [[entities/client-manager]] (`ClientManager`) manages stateful external client WebSocket connections (`Set<WebSocket>`). It acts as the egress point, shielding the internal Kafka cluster and the OME from external connections. It performs protocol translation (from internal Protobuf to external JSON strings) before broadcasting, as detailed in [[decisions/protocol-translation]].

4. **Warm-Start State Recovery**:
   Instead of forcing new clients to wait for the next periodic Kafka snapshot broadcast, the gateway integrates with a [[entities/redis-cache-layer]]. The latest L2 state is continuously cached, allowing clients to recover instantly upon initial connection. This strategy is formalised in [[decisions/state-ingestion-recovery]].

5. **Strict Separated Failure Domains**:
   If a spike of 10,000 WebSocket clients connect or disconnect simultaneously, only the [[entities/client-manager]] and the node running the gateway are affected. The OME continues to match trades at sub-millisecond speeds, completely unaffected by public network fluctuations.

## Consequences

### Positive
* **No Backpressure Propagation**: The OME can run at its maximum throughput. Slow TCP connections to external clients have no performance feedback loop to the matching core.
* **Elastic Scalability**: The Market Data Gateway layer (including the consumers and WebSocket servers) can be scaled horizontally behind an external load balancer to handle millions of concurrent subscribers.
* **Persistence & Offlining**: The [[entities/kafka-message-bus]] guarantees persistence. If a gateway node restarts, the consumer groups can replay events from their last committed offsets.
* **Clean Code Boundary**: Clean separation of domains inside [[concepts/data-flow-pipelines]]: binary protocol ingestion remains entirely separated from JSON-based public distribution.

### Negative / Trade-offs
* **Increased System Complexity**: Adding Kafka and consumer groups introduces additional moving parts to monitor, provision, and maintain.
* **Additional Transport Latency**: The jump through Kafka adds a microsecond-to-millisecond network hop penalty. This is acceptable, as public market data egress does not reside on the latency-critical trading path.
* **Fairness Jitter**: Sequential iteration over active client websockets inside the [[entities/client-manager]] introduces minor timing discrepancies. These challenges and strategies to combat them are handled in [[decisions/sequential-broadcast-fairness]].

## References
* High-Level Data Ingestion & Flow: [[concepts/data-flow-pipelines]]
* Client Connection Layer: [[concepts/websocket-egress]]
* Internal Telemetry Formats: [[entities/protobuf-schema]]