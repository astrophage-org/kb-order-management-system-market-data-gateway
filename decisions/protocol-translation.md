# Architectural Decision Record: Protocol Translation — Binary Protobuf Ingress vs. JSON Egress

## Status

**Accepted**

---

## Context

The Nexus Trading Exchange (NTE) platform demands extreme performance. The core matching layer must achieve sub-millisecond execution times, while the downstream [[summaries/market-data-gateway]] (MDG) is responsible for distributing real-time public market data to thousands of concurrent external subscribers. 

To achieve this, the system is split into two distinct networking boundaries:

1. **The Internal Network Boundary (Ingress):** Under high-throughput conditions, the [[entities/order-matching-engine]] publishes high-frequency updates—specifically [[concepts/level-2-depth]] snapshots and [[concepts/trade-tick-event]] executions—to the [[entities/kafka-message-bus]] via the topics [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-orderbook-snapshots|nte.orderbook.snapshots]] and [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-trades-matched|nte.trades.matched]]. 
2. **The External Network Boundary (Egress):** The [[entities/client-manager]] broadcasts these events to external public clients over [[concepts/websocket-egress]].

Historically, using a single serialization format end-to-end (such as pure JSON or pure Protobuf) has presented a difficult trade-off:
* **Pure JSON End-to-End:** Simplifies downstream client usage, but introduces massive serialization CPU tax on the [[entities/order-matching-engine]] and inflates payload sizes over the internal [[entities/kafka-message-bus]], consuming internal network bandwidth.
* **Pure Protobuf End-to-End:** Offers exceptional performance and minimal payload sizes across all segments, but forces external API integrations to compile and maintain `.proto` client files, creating a significant adoption barrier for web-based UI clients and third-party algorithmic traders.

This decision record evaluates the trade-offs of adopting a **hybrid protocol translation architecture**: utilizing binary protocol buffers on internal ingress streams and translating them to JSON strings on external egress broadcasts.

---

## Decision

We will implement a hybrid **Binary Protobuf Ingress to JSON Text Egress** protocol translation model within the [[concepts/data-flow-pipelines|Data Flow Pipelines]].

```
[Matching Engine]
       │
       │ (Serializes using Protobuf Schema)
       ▼
 [Kafka Topic] ──► [Consumers] ──► [Deserialized Domain Models] ──► [Client Manager] ──► [External Client]
                  (Protobuf)        (TypeScript Interface Types)     (JSON Stringified)    (WebSocket JSON)
```

### 1. Ingress Protocol (Internal)
All data published from the [[entities/order-matching-engine]] over the [[entities/kafka-message-bus]] will use binary [[entities/protobuf-schema]] serialization. 
* The internal contract is strictly defined by `protos/market_data.proto` (`nte.marketdata.OrderBookSnapshot` and `nte.marketdata.TradeTick`).
* The [[entities/order-book-consumer]] and [[entities/trade-consumer]] ingest these binary payloads directly from Kafka.

### 2. Intermediate Domain Model representation
Once consumed, the binary payloads are immediately deserialized inside the consumer workers into standard, lightweight TypeScript domain models (defined in `src/models/types.ts` as `OrderBookEvent` and `TradeEvent`). This isolates the network schema from our application memory layout.

### 3. Egress Protocol (External)
Before broadcasting, the [[entities/client-manager]] converts these native memory structures into standard JSON strings (e.g., matching the `L2_UPDATE` and `TRADE_TICK` schemas). The resulting JSON is distributed over [[concepts/websocket-egress]] connections.

---

## Trade-offs & Consequences

### Advantages

* **Matching Engine Efficiency:** The critical path of the [[entities/order-matching-engine]] is completely insulated from expensive string-formatting operations. Serializing to binary Protobuf is significantly faster and uses less CPU than stringifying nested JSON structures (especially massive L2 order book arrays).
* **Network & Disk Savings on Kafka:** Using binary serialization reduces Kafka payload sizes by up to 70–80% compared to equivalent verbose JSON, reducing network I/O overhead and physical storage requirements on the [[entities/kafka-message-bus]] brokers.
* **Client Accessibility:** Public API consumers do not need specific SDKs or deserialization libraries. They can establish standard WebSocket connections and immediately parse JSON messages, which is natively supported by all major browsers, languages, and trading scripts.
* **Separation of Concerns via Decoupling:** By performing protocol translation inside the MDG, we maintain clean separation as outlined in [[decisions/decoupling-via-pub-sub]]. The core engine remains blissfully unaware of external client requirements.

### Disadvantages & Mitigations

* **Protocol Translation CPU Overhead:** The gateway must perform CPU-bound translation (Protobuf Parse -> Native Object -> JSON Stringify). 
  * *Mitigation:* This translation overhead is offloaded entirely onto the MDG consumer instances, which can be scaled horizontally behind load balancers. The core matching engine remains unaffected.
* **Latency Jitter:** Converting to JSON strings on the fly inside the sequential loop of the [[entities/client-manager]] can introduce latency spikes during microsecond measurements, as detailed in [[decisions/sequential-broadcast-fairness]].
  * *Mitigation:* The JSON stringification occurs exactly *once* per event payload before the broadcast loop, rather than per client connection (e.g., `broadcastOrderBook` takes a pre-stringified snapshot or stringifies it once, then sends the identical buffer to all open client sockets).
* **System Complexity:** Two schemas must be maintained—the binary Protobuf schemas and the JSON domain contracts.
  * *Mitigation:* Solid integration tests ensure that any change to `protos/market_data.proto` is verified against the TypeScript type definitions in `src/models/types.ts`.

---

## Related Architectural Context
* To handle instantaneous state recovery without forcing new clients to wait for the next sequential broadcast, this protocol translation boundary interacts with the [[entities/redis-cache-layer]]. The latest L2 state is captured and stored in a quickly retrievable format, as explained in [[decisions/state-ingestion-recovery]].