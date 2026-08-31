# Caching L2 Snapshot Updates in Redis for Instant Client Recovery

## Status

**Accepted**

## Context

The Market Data Gateway (MDG) is responsible for distributing real-time public market data—specifically [[concepts/level-2-depth|Level 2 (L2) order books]] and [[concepts/trade-tick-event|trade ticks]]—to external clients via [[concepts/websocket-egress|WebSocket connections]]. 

Under the decoupled design established in [[decisions/decoupling-via-pub-sub]], the [[entities/order-book-consumer]] consumes binary-encoded snapshots from the [[entities/kafka-message-bus]] on the topic [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-orderbook-snapshots|order-matching-engine (nte.orderbook.snapshots)]] (defined in [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-orderbook-snapshots|order-matching-engine (nte.orderbook.snapshots)]]). These snapshots are periodically published by the [[entities/order-matching-engine]].

When an external client establishes a new connection to the [[entities/client-manager]], they face a synchronization gap. The client cannot interpret delta updates without an initial, full state baseline. If they must wait for the next cyclic broadcast on the Kafka topic to arrive, they will experience a high "time-to-first-book" delay (potentially up to several seconds under low-volatility conditions). This latency degrades user experience and delays trading execution.

Directly querying the [[entities/order-matching-engine]] for synchronous snapshots on every client connection is not viable, as it violates the core decoupling principle and risks degrading the performance of the matching engine.

## Decision

We will implement an in-memory caching mechanism using the [[entities/redis-cache-layer]] to store the latest state of the [[concepts/level-2-depth|L2 order book]] for every active symbol. This cache will act as a state-recovery system for newly connected clients, facilitating a "warm-start".

```
                          +-----------------------------------+
                          |     `Order Matching Engine`     |
                          +-----------------+-----------------+
                                            |
                                            | (Publishes)
                                            v
                          +-----------------------------------+
                          |       `Kafka Message Bus`       |
                          |      nte.orderbook.snapshots      |
                          +-----------------+-----------------+
                                            |
                                            | (Consumes)
                                            v
                          +-----------------------------------+
                          |    `Order Book Consumer`        |
                          +--------+-----------------+--------+
                                   |                 |
                  (Updates Cache)  |                 | (Passes to Client Manager)
                                   v                 v
                 +-------------------+     +-------------------+
                 | `Redis Cache Layer` |     | `Client Manager`|
                 +---------+---------+     +---------+---------+
                           ^                         |
                           | (Fetches on Connection) |
                           +-------------------------+
                                                     |
                                                     | (Sends Initial Warm-Start)
                                                     v
                                            +-----------------+
                                            | External Client |
                                            +-----------------+
```

### 1. Ingestion and Write Pipeline
The [[entities/order-book-consumer]] will handle the dual write-path:
1. **Decode & Broadcast:** The consumer processes the binary payload from Kafka using the [[entities/protobuf-schema]], translates it to JSON (per [[decisions/protocol-translation]]), and forwards it to the [[entities/client-manager]] for broadcasting.
2. **Cache Update:** Simultaneously, the consumer asynchronously updates the [[entities/redis-cache-layer]]. The serialized JSON payload representing the current snapshot is stored in Redis under a structured key:
   ```
   nte:l2:snapshot:<symbol>
   ```
   *Note: Storing the already-serialized JSON payload in Redis optimizes reading speed, avoiding repetitive CPU-bound serialization steps when multiple new clients connect.*

### 2. Client Connection & Warm-Start Pipeline
When a new client establishes a connection through the [[entities/client-manager]]:
1. The WebSocket connection is registered.
2. The [[entities/client-manager]] immediately queries the [[entities/redis-cache-layer]] for the latest snapshot of the requested symbol(s).
3. Redis returns the pre-serialized JSON string.
4. The [[entities/client-manager]] sends this warm-start snapshot directly to the newly connected socket over [[concepts/websocket-egress]].
5. Subsequent real-time delta updates are merged naturally by the client, using the sequence ID or timestamp as a reconciliation boundary.

### 3. Key Expiry and Cache Eviction
To prevent stale or dead instruments from consuming memory indefinitely:
* Snapshots in the [[entities/redis-cache-layer]] will be written with a Time-To-Live (TTL) of **24 hours**.
* The TTL is reset on every successful write. If an instrument does not receive any L2 book updates for 24 hours (e.g., expired or delisted contracts), it is purged automatically from Redis.

## Consequences

* **Instantaneous Synchronization:** New clients receive the full market state within milliseconds of opening a WebSocket connection, bypassing the waiting time for the next scheduled Kafka snapshot.
* **Low Impact on Hot Path:** Writing to Redis from the [[entities/order-book-consumer]] adds sub-millisecond, non-blocking I/O overhead that does not impact the critical broadcasting path.
* **Preserved Decoupling:** The [[entities/order-matching-engine]] remains completely insulated from client churn and connection scaling issues.
* **State Recovery:** If a Market Data Gateway instance crashes, the newly booted replacement pod does not need to wait for Kafka history or request state replays; it can immediately serve clients by reading directly from the centralized [[entities/redis-cache-layer]].

## See Also
* [[index]]
* [[concepts/data-flow-pipelines]]
* [[decisions/protocol-translation]]
* [[decisions/decoupling-via-pub-sub]]