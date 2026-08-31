<!-- anchor: src/index.ts:L1-L100 sha:HEAD -->

# Client Manager

The `ClientManager` is the stateful, network-facing core of the [[summaries/market-data-gateway|Market Data Gateway (MDG)]]. Implemented in `src/websockets/ClientManager.ts`, it initiates and maintains a high-performance WebSocket server, acts as the centralized registry for active external client connections, and provides optimized broadcast interfaces for real-time market updates. 

The `ClientManager` decouples downstream client connection management and message delivery from the internal ingestion consumers, establishing a core link in the gateway's [[concepts/websocket-egress|WebSocket Egress]] mechanism.

---

## ## Responsibilities

The `ClientManager` is charged with several key tasks in the distribution of market telemetry:

1. **Active Connection Registry**: Maintains a dynamic, low-overhead in-memory registry (`Set<WebSocket>`) of all actively subscribed external client sessions.
2. **Lifecycle Management**: Safely handles new connections and client disconnections, guaranteeing instant cleanup via the `close` listener to prevent memory leaks and dangling socket references.
3. **Downstream Protocol Translation**: Converts raw data strings (originally received from the [[entities/protobuf-schema|internal Protobuf-derived stream]]) and encapsulates them into standardized, public-facing client JSON payloads as defined in the [[decisions/protocol-translation|Protocol Translation Decision]].
4. **Targeted Broadcast Pipelines**:
   * **`broadcastOrderBook`**: Formats and broadcasts [[concepts/level-2-depth|Level 2 (L2) depth updates]] using the `L2_UPDATE` payload envelope.
   * **`broadcastTrade`**: Formats and broadcasts [[concepts/trade-tick-event|trade tick events]] using the `TRADE_TICK` payload envelope.
5. **Fair and Fast Distribution**: Iterates over registered sockets to broadcast updates simultaneously, striving to satisfy the MDG's high-level mandate of market data fairness.

---

## ## Dependencies

The `ClientManager` acts as an integration hub, relying on or feeding into several system components:

* **`ws` Library**: Uses the lightweight `ws` npm library to manage underlying TCP/WebSocket connections (`WebSocketServer` and `WebSocket`).
* **[[entities/order-book-consumer|OrderBookConsumer]]**: Feeds deserialized data from the [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-orderbook-snapshots|order-matching-engine (nte.orderbook.snapshots)]] Kafka topic into `ClientManager.broadcastOrderBook()`.
* **[[entities/trade-consumer|TradeConsumer]]**: Feeds trade executions from the [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-trades-matched|order-matching-engine (nte.trades.matched)]] Kafka topic into `ClientManager.broadcastTrade()`.
* **[[index|Bootstrap Layer (`src/index.ts`)]]**: Spawns and configures the `ClientManager` on port `8080`.
* **[[entities/redis-cache-layer|Redis Cache Layer]] *(Planned Integration)***: Will collaborate with `ClientManager` during initial connection handshakes, allowing new clients to retrieve a warm L2 snapshot on-demand without waiting for the next cyclic Kafka broadcast (see [[decisions/state-ingestion-recovery|State Ingestion & Recovery Decision]]).

---

## Code Architecture and API

The `ClientManager` is written in TypeScript and exposes a simple, streamlined API designed for minimal invocation overhead:

```typescript
import { WebSocketServer, WebSocket } from 'ws';

export class ClientManager {
    private wss: WebSocketServer;
    private clients: Set<WebSocket> = new Set();

    constructor(port: number) {
        this.wss = new WebSocketServer({ port });
        this.wss.on('connection', (ws) => {
            this.clients.add(ws);
            ws.on('close', () => this.clients.delete(ws));
        });
    }

    broadcastOrderBook(snapshot: string) {
        for (const client of this.clients) {
            if (client.readyState === WebSocket.OPEN) {
                client.send(JSON.stringify({ type: 'L2_UPDATE', data: snapshot }));
            }
        }
    }

    broadcastTrade(trade: string) {
        for (const client of this.clients) {
            if (client.readyState === WebSocket.OPEN) {
                client.send(JSON.stringify({ type: 'TRADE_TICK', data: trade }));
            }
        }
    }
}
```

### Protocol Conversion & Egress Flow

```
   ┌────────────────────────────────────────────────────────┐
   │                  Inbound Kafka Events                  │
   │  - [[entities/order-book-consumer|OrderBookConsumer]] (L2 Snapshots)  │
   │  - [[entities/trade-consumer|TradeConsumer]] (Trade Executions)        │
   └───────────────────────────┬────────────────────────────┘
                               │ (Invokes Broadcast Methods)
                               ▼
   ┌────────────────────────────────────────────────────────┐
   │                    ClientManager                       │
   │  - Iterates Open Sockets in client Set                 │
   │  - Serializes data as JSON {type, data}                │
   └───────────────────────────┬────────────────────────────┘
                               │
                ┌──────────────┼──────────────┐
                │ (JSON Send)  │ (JSON Send)  │ (JSON Send)
                ▼              ▼              ▼
           [Client 1]     [Client 2]     [Client N]
```

---

## Architectural Considerations

### 1. Sequential Broadcast vs. Microsecond Fairness
The current broadcast implementation relies on a sequential iteration loop over the active `clients` Set. Under extreme volumes or high client counts, this loop is susceptible to "head-of-line sweeping latency," where clients near the end of the Set receive events slightly later than those at the beginning. 

While sufficient for current operational requirements, mitigating this microsecond-level jitter is an area of ongoing optimization. Planned improvements include parallelized socket group dispatches and kernel-level zero-copy optimizations, as outlined in [[decisions/sequential-broadcast-fairness|Sequential Broadcast & Latency Fairness]].

### 2. Upstream Decoupling
Because the `ClientManager` receives its events exclusively from the [[entities/order-book-consumer|OrderBookConsumer]] and [[entities/trade-consumer|TradeConsumer]] via the [[entities/kafka-message-bus|Kafka Message Bus]], the core [[entities/order-matching-engine|Order Matching Engine]] is completely insulated from client slow-consumers, transport handshakes, and network backpressure. This design decision is further discussed in [[decisions/decoupling-via-pub-sub|Decoupling via Pub-Sub]].

### 3. Protocol Translation Costs
The transition from internal binary schema payloads (such as [[entities/protobuf-schema|Protobuf]]) to public JSON egress ensures that API developers can easily consume the streams using native browser or server WebSockets. The serialization tax of `JSON.stringify` on every broadcast remains localized to the gateway layer and does not impact the critical internal trading path (see [[decisions/protocol-translation|Protocol Translation: Binary Ingress vs. Text Egress]]).