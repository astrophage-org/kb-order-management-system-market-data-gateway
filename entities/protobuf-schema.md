<!-- anchor: README.md:L1-L100 sha:HEAD -->

# Protocol Buffers Schema (`market_data.proto`)

The `@astrophage/market-data-gateway` relies on Google Protocol Buffers (v3) to establish a strict, language-agnostic data contract for internal high-throughput telemetry. Defined in `protos/market_data.proto`, this schema serves as the single source of truth for raw binary event payloads written to the [[entities/kafka-message-bus]] by the upstream [[entities/order-matching-engine]].

By enforcing binary serialization at the ingestion layer, the sub-system guarantees minimal network serialization footprints and optimal CPU execution profiles across the core exchange infrastructure, prior to translating payloads to JSON text format for external consumption.

---

## ## Responsibilities

The primary responsibilities of the `market_data.proto` schema within the Nexus Trading Exchange (NTE) platform are:
*   **Contract Enforcement:** Providing a unified payload format shared between the upstream engine (implemented in high-performance compiled languages) and downstream gateways (such as Node.js).
*   **Serialization Optimization:** Minimizing message sizes across internal high-volume topics like [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-orderbook-snapshots|order-matching-engine (nte.orderbook.snapshots)]] and [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-trades-matched|order-matching-engine (nte.trades.matched)]].
*   **Precision Preservation:** Ensuring nanosecond-precision timestamps (`timestamp_ns`, `matched_at_ns`) and double-precision floating-point price/quantity boundaries remain uncorrupted during cross-network transport.
*   **Polyglot Compilation Target:** Exposing target compilation directories for multiple language runtimes, configured via:
    *   `option java_package = "com.gfmg.nte.marketdata";`
    *   `option go_package = "github.com/astrophage/market-data-gateway/protos";`

---

## ## Dependencies

The Protobuf schema operates at the interface layer of the Market Data Gateway and is linked to the following systems and topics:

*   **Upstream Producers:**
    *   [[entities/order-matching-engine]] — Instantiates the binary serialization logic and publishes directly to Kafka.
    *   [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-orderbook-snapshots|order-matching-engine (nte.orderbook.snapshots)]] — Interface topic supplying binary `OrderBookSnapshot` payloads.
    *   [[ap:kb-order-management-system-order-matching-engine/summaries/order-matching-engine#nte-trades-matched|order-matching-engine (nte.trades.matched)]] — Interface topic supplying binary `TradeTick` payloads.
*   **Downstream Consumers (MDG):**
    *   [[entities/order-book-consumer]] — Consumes and deserializes binary `OrderBookSnapshot` updates.
    *   [[entities/trade-consumer]] — Consumes and deserializes binary `TradeTick` matches.
*   **Infrastructure Layers:**
    *   [[entities/kafka-message-bus]] — Transports raw Protobuf-encoded bytes across system boundaries.
    *   [[decisions/protocol-translation]] — Strategic architectural decision governing the translation of Protobuf binary structures into JSON text format before transmitting down to the [[concepts/websocket-egress]].

---

## Schema Specification

The complete `protos/market_data.proto` specification is configured as follows:

```protobuf
syntax = "proto3";
package nte.marketdata;

option java_package = "com.gfmg.nte.marketdata";
option go_package = "github.com/astrophage/market-data-gateway/protos";

// Consumed from nte.orderbook.snapshots topic published by order-matching-engine
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

// Consumed from nte.trades.matched topic published by order-matching-engine
message TradeTick {
    string trade_id = 1;
    string symbol = 2;
    double price = 3;
    double quantity = 4;
    int64 matched_at_ns = 5;
    string taker_side = 6;
}
```

---

## Message Analysis & Field Details

### `OrderBookSnapshot` & `Level`
Defines a point-in-time state representational snapshot of [[concepts/level-2-depth]]. 

| Field Name | Type | Key Architecture Purpose |
| :--- | :--- | :--- |
| `symbol` | `string` | System ticker identifier (e.g., `BTCUSD`). |
| `bids` | `repeated Level` | Array of aggregated buy orders ordered by price descending. |
| `asks` | `repeated Level` | Array of aggregated sell orders ordered by price ascending. |
| `timestamp_ns`| `int64` | Nanosecond epoch timestamp indicating when the engine calculated the snapshot. |

Each `Level` represents a aggregated price tier:
*   `price` (`double`): The target price boundary.
*   `quantity` (`double`): Total aggregate volume outstanding at this level.
*   `order_count` (`int32`): Count of unique resting orders inside this level (critical for identifying market depth thickness).

### `TradeTick`
Defines a completed match event resulting from a taker order hitting the order book. This is the source schema for [[concepts/trade-tick-event]].

| Field Name | Type | Key Architecture Purpose |
| :--- | :--- | :--- |
| `trade_id` | `string` | Unique global execution identifier generated by the engine. |
| `symbol` | `string` | Ticker identifier. |
| `price` | `double` | Execution price of the match. |
| `quantity` | `double` | Volume transacted. |
| `matched_at_ns`| `int64` | Nanosecond precision matching timestamp. |
| `taker_side` | `string` | Classification of the aggressing side (`BUY` or `SELL`). Used to determine tick direction. |

---

## Data Translation Pipeline

To handle external client distribution, MDG acts as a high-performance **protocol converter**. Raw binary packets are decoded and reformatted to match internal TypeScript interfaces prior to JSON conversion:

```
                  [Protobuf Ingress]
            (protos/market_data.proto)
             - OrderBookSnapshot
             - TradeTick
                      │
                      │ (Parsed by OrderBookConsumer & TradeConsumer)
                      ▼
               [Domain Models] (src/models/types.ts)
                - PriceLevel
                - OrderBookEvent
                - TradeEvent
                      │
                      │ (Serialised by ClientManager)
                      ▼
              [JSON Client Egress]
               - L2_UPDATE
               - TRADE_TICK
```

1.  **Ingress Parsing:** The binary streams are received from Kafka. Inside `OrderBookConsumer` and `TradeConsumer`, the buffers are converted into corresponding memory entities.
2.  **Domain Matching:** The raw message fields match the schema interfaces defined in `src/models/types.ts`.
3.  **Client Egress:** The `ClientManager` processes the domain models into stringified JSON structures (`L2_UPDATE` and `TRADE_TICK`) for the final websocket push, ensuring compatibility for front-end SDKs as documented in [[decisions/protocol-translation]].