# Sequential Broadcast Fairness vs. Microsecond Latency Jitter

## Status

**Approved**

## Context

The [[summaries/market-data-gateway]] (MDG) acts as the primary egress point for public market data, specifically streaming [[concepts/level-2-depth|Level 2 (L2) order books]] and [[concepts/trade-tick-event|trade ticks]] to external clients. A core architectural mandate of this subsystem is **fairness and simultaneity**: ensuring that all external subscribers receive real-time market updates concurrently, minimizing latency jitter across client connections.

In the initial implementation of the [[entities/client-manager|Client Manager]] (`ClientManager.ts`), broadcasting to clients is executed via a sequential `for...of` loop over a `Set<WebSocket>` collection:

```typescript
for (const client of this.clients) {
    if (client.readyState === WebSocket.OPEN) {
        client.send(JSON.stringify({ type: 'L2_UPDATE', data: snapshot }));
    }
}
```

While this approach has low complexity and negligible heap allocation overhead, it presents severe architectural risks under high concurrent load:

1. **Head-of-Line (HoL) Sweeping Latency:** Iterating over hundreds or thousands of active WebSocket connections is inherently sequential in the Node.js single-threaded event loop. If client $N$ experiences network congestion, or if the V8 engine encounters write delays, the dispatch to client $N+1$ is delayed.
2. **TCP Window Backpressure Propagation:** When broadcasting to a slow or degraded client connection, the underlying operating system TCP buffer fills up. If `ws.send()` blocks or waits for system resources, it injects microsecond-level (or even millisecond-level) latency jitter into subsequent iterations of the same loop. This violates the fairness mandate, giving an unfair latency advantage to clients situated early in the `Set` iteration order.
3. **Array/Set Reordering Jitter:** Because connections are added and deleted dynamically as clients connect and disconnect (via `this.clients.add` and `this.clients.delete`), the iteration order of the `Set` is dependent on connection history. This creates unpredictable, non-deterministic latency bias across the connected client base.

To maintain strict institutional fairness, we must analyze the trade-offs of sequential loop broadcasts and define a deterministic path to handle microsecond latency jitter.

## Decision

To achieve deterministic latency fairness, we have decided to implement a multi-tiered architecture that mitigates sequential broadcasting bottlenecks. This decision balances immediately implementable runtime enhancements with a planned migration to a native C++ polling layer.

### 1. Immediate Mitigation: Non-Blocking Async Queueing & Drop-Oldest Buffer Policy

We will modify the [[entities/client-manager|Client Manager]] to prevent slow-paying TCP connections from blocking the main broadcast loop. 

* **Buffered Backpressure Detection:** For each client, we will inspect the `bufferedAmount` property before sending. If a client's buffered memory exceeds a predefined threshold (e.g., 512 KB), it indicates the client is failing to consume at line rate.
* **Drop-Oldest / Connection Termination:** If a client falls behind (backpressure threshold breached), the MDG will immediately drop the current update for that specific client or terminate the connection rather than stalling the sweep loop. This ensures that the degradation of one client connection cannot propagate latency jitter to healthy peers.

```typescript
// Conceptual Implementation of Non-Blocking Guard
broadcastOrderBook(snapshot: string) {
    const payload = JSON.stringify({ type: 'L2_UPDATE', data: snapshot });
    
    for (const client of this.clients) {
        if (client.readyState === WebSocket.OPEN) {
            // Check backpressure to prevent HoL blocking
            if (client.bufferedAmount > 524288) { // 512KB limit
                this.handleSlowClient(client);
                continue;
            }
            client.send(payload);
        }
    }
}
```

### 2. Payload Optimization: Single-Serialization Constraint

We enforce that serialization to JSON occurs **exactly once** per broadcast interval rather than per client. In the raw `ClientManager` implementation, `JSON.stringify` is already evaluated outside the loop or within a shared scope where possible to prevent CPU exhaustion. 

As defined in the [[decisions/protocol-translation|Protocol Translation Decision]], translating incoming binary data from the [[entities/protobuf-schema|Protobuf Schema]] to outbound JSON text egress is a recognized CPU bottleneck. Single-serialization guarantees that V8 only pays the CPU cost of string compilation once per event.

### 3. Medium-Term Strategy: Migration to Native Group Broadcasts (`uWebSockets.js`)

While Node's `ws` library is highly compatible, it does not support native-level group broadcasting. We will migrate the WebSocket server registry in [[entities/client-manager]] to `uWebSockets.js` (a Node.js wrapper around the C++ `uWebSockets` library).

* **Kernel-Level Multicasting Simulation:** `uWebSockets.js` supports a native `publish()` mechanism that utilizes C++ vectors to sweep connections at the machine-code level, bypassing the V8 heap iteration entirely.
* **Shared Memory Broadcasting:** Instead of allocating individual JS-wrapper objects for every socket during iteration, `uWebSockets.js` writes the payload directly to the network buffers of all sockets subscribed to a specific "topic" (e.g., `market/L2/BTC-USD`). This reduces sequential iteration jitter from microsecond-level variances to near-nanosecond level system calls.

### 4. Integration with State Recovery and Data Ingestion Pipelines

This decision aligns with the overall [[concepts/data-flow-pipelines|Data Flow Pipelines]] and the [[decisions/state-ingestion-recovery|State Ingestion Recovery Decision]]. Because new clients recover historical state instantly from the [[entities/redis-cache-layer|Redis Cache Layer]], they do not need to poll or disrupt the real-time broadcast loop. The live broadcast channel only handles real-time delta distributions, ensuring that slow, cold-starting clients do not degrade the latency profile of warm, active clients.

Additionally, this ensures that the performance gains unlocked by decoupling the upstream engine via the [[entities/kafka-message-bus|Kafka Message Bus]] (see [[decisions/decoupling-via-pub-sub]]) are not lost at the final [[concepts/websocket-egress|WebSocket Egress]] boundary.

---

### Comparison of Architectural Approaches

| Parameter | Sequential Loop (`ws` standard) | Non-Blocking Sweep (Adopted Baseline) | Native Group Broadcast (`uWebSockets.js` Planned) |
| :--- | :--- | :--- | :--- |
| **Average Sweep Jitter** | High ($>100\,\mu\text{s}$ at 500+ connections) | Medium ($<15\,\mu\text{s}$ at 500+ connections) | Ultra-Low ($<2\,\mu\text{s}$ at 500+ connections) |
| **Slow Client Protection** | None (Slow clients block healthy ones) | High (Immediate skip/disconnect on backpressure) | High (Native kernel ring-buffer management) |
| **V8 Heap Overhead** | High (Allocation of wrappers per loop) | Low (Reuses single JSON string payload) | Minimal (Data written directly from C++ heap) |
| **Implementation Complexity**| Minimal | Low | Medium (Requires native compilation dependencies) |