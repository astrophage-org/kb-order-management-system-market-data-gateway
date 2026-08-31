<!-- anchor: docs/architecture.md:L1-L100 sha:HEAD -->

# Order Book Consumer

The `OrderBookConsumer` is a high-performance, dedicated Kafka consumer component within the [[summaries/market-data-gateway]]. It is responsible for subscribing to the L2 order book snapshot streams emitted by the upstream [[entities/order-matching-engine]], translating the payload formats, and immediately forwarding the state updates to the [[entities/client-manager]] for active client distribution.

By managing the intake of these large snapshot volumes in a decoupled consumer group (`mdg-orderbook-group`), the system isolates trading state distribution from trade tick consumption, which is handled in parallel by the [[entities/trade-consumer]].

---

## Responsibilities

The `OrderBookConsumer` class performs several critical lifecycle and data processing operations:

*   **Kafka Lifecycle Management:** Connects to the [[entities/kafka-message-bus]] as a member of the stateful `mdg-orderbook-group` using the `kafkajs` client library.
*   **Topic Subscription:** Listens specifically to the [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-orderbook-snapshots|order-matching-engine (nte.orderbook.snapshots)]] topic (configured via environmental variables) starting from the latest offset (`fromBeginning: false`) to avoid flooding active clients with stale historical depth updates.
*   **Payload Parsing (Protocol Translation):** Ingests binary snapshots defined by the [[entities/protobuf-schema]] (`market_data.proto`). It handles the deserialization pathway to convert the raw buffer back into structured L2 book components before serialization to JSON for WebSocket egress (as detailed in [[decisions/protocol-translation]]).
*   **Egress Delegation:** Hands off parsed order book state directly to the `ClientManager` to trigger client broadcasts.

---

## Dependencies

The `OrderBookConsumer` operates as a mid-pipeline processor and relies on the following internal and external components:

### Upstream Dependencies
*   **[[entities/order-matching-engine]] / Kafka Topic:** Consumes data generated from the core matching logic.
    *   External reference: [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-orderbook-snapshots|order-matching-engine (nte.orderbook.snapshots)]]
*   **[[entities/kafka-message-bus]]:** The underlying streaming backbone providing broker clustering and consumer group synchronization.

### Downstream Dependencies
*   **[[entities/client-manager]] (`ClientManager`):** Receives the processed payload and handles the subsequent execution of sequential socket writes.
*   **[[entities/redis-cache-layer]]:** Works in conjunction with the cache to ensure that the consumed L2 snapshot is persisted for new clients seeking immediate state synchronization (see [[decisions/state-ingestion-recovery]]).

### Schema Dependencies
*   **[[entities/protobuf-schema]] (`market_data.proto`):** Governs the incoming binary stream payload shape (`OrderBookSnapshot`).
*   **Domain Types (`src/models/types.ts`):** Models the in-memory representation (`OrderBookEvent` and `PriceLevel`) within the Node.js runtime environment.

---

## Technical Architecture & Ingestion Flow

The consumer handles data flow along **Path A** of the [[concepts/data-flow-pipelines]] architecture.

```
┌─────────────────────────────────┐
│     Order Matching Engine       │
└────────────────┬────────────────┘
                 │ (Publishes)
                 ▼
┌─────────────────────────────────┐
│  Topic: nte.orderbook.snapshots │
└────────────────┬────────────────┘
                 │
                 │ (Polls partition)
                 ▼
┌─────────────────────────────────┐
│       OrderBookConsumer         │
│  - Client: mdg-ob-consumer      │
│  - Group: mdg-orderbook-group   │
└────────────────┬────────────────┘
                 │
                 │ (Deserializes Protobuf -> String)
                 ▼
┌─────────────────────────────────┐
│         Client Manager          │
│ (Translates to JSON & Broadcast)│
└────────────────┬────────────────┘
                 │
                 ▼
         [[concepts/websocket-egress]]
```

### Protocol Buffers Ingress
The upstream matching engine serializes the [[concepts/level-2-depth]] snapshot using the `nte.marketdata.OrderBookSnapshot` schema. This keeps the internal network footprint extremely lean:

```protobuf
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

Upon receipt, the consumer converts these bids and asks levels into the standard domain format (`PriceLevel[]`) required for client delivery. This separation of concerns enforces architectural boundary rules where binary models reside internally, and text-based JSON representations are passed externally (see [[decisions/decoupling-via-pub-sub]]).

---

## Code Implementation

The consumer is implemented in TypeScript under `src/consumers/OrderBookConsumer.ts`. Below is the core runtime logic of the class:

```typescript
import { Kafka } from 'kafkajs';
import { ClientManager } from '../websockets/ClientManager';

export class OrderBookConsumer {
    private kafka = new Kafka({ 
        clientId: 'mdg-ob-consumer', 
        brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'] 
    });
    private consumer = this.kafka.consumer({ groupId: 'mdg-orderbook-group' });

    constructor(private clientManager: ClientManager) {}

    async start() {
        await this.consumer.connect();
        // Consuming snapshots generated by astrophage/order-matching-engine
        await this.consumer.subscribe({ topic: 'nte.orderbook.snapshots', fromBeginning: false });

        await this.consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                // TODO: Deserialize Protobuf message mapping bytes to OrderBookEvent
                const snapshot = message.value?.toString(); 
                if (snapshot) {
                    this.clientManager.broadcastOrderBook(snapshot);
                }
            }
        });
    }
}
```

### Key Implementation Details
1.  **Consumer Group Strategy (`mdg-orderbook-group`):** Assigning a dedicated group ID allows Kafka to dynamically balance partitions among multiple scaling instances of the Market Data Gateway without interfering with other consumer pipelines like `mdg-trade-group`.
2.  **Immediate Dispatch:** The decoded string/JSON format is instantly pushed to the `ClientManager`. The client manager broadcasts the event across active TCP/WebSocket connections.
3.  **Low Latency Trade-offs:** The broadcast utilizes a fast sequential iteration loop. For details regarding microsecond jitter and potential mitigation strategies, review [[decisions/sequential-broadcast-fairness]].