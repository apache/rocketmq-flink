# gRPC Connector

The gRPC connector (`flink-connector-rocketmq-grpc`) targets RocketMQ 5.x using
`rocketmq-client-java` 5.2.1+ and offers two consumption modes. In **SIMPLE** mode — the default —
the source subscribes to a normal topic (with an optional tag / SQL92 filter) and acknowledges the
messages itself once the enclosing checkpoint completes. In **LITE** mode, which must be selected
explicitly, the source binds one main lite topic with a wildcard subscription, never acks by itself,
and hands the ack / throttle decision to downstream operators. Section 2 details both modes.

This document covers both the design (architecture, semantics, decisions and trade-offs) and the
usage of the connector.

---

## 1. Motivation

The target scenario is a **main lite topic with a large number of sub (lite) topics** underneath,
e.g. AI workloads where every tenant/session gets its own sub topic:

- The source subscribes to the main topic with a **wildcard (generalized) subscription**; the
  broker funnels messages of *all* sub topics through one `receive()` stream, so the reader never
  has to enumerate or track sub topics.
- Whether a message should be **acknowledged** or **deferred (throttled)** is a business decision
  that is only known *after* downstream processing — possibly on the other side of a
  keyBy/shuffle. A hot sub topic that consumes too many resources should be slowed down without
  starving the other sub topics.

This leads to the two defining properties of the connector:

1. **The source never acknowledges messages.** Acknowledgement is deferred to downstream
   operators, which receive a self-contained, serializable receipt handle with every record.
2. **Throttling is expressed per message via `changeInvisibleDuration`.** Deferring the
   redelivery of a hot sub topic's messages naturally yields fair sharing of the fetch budget:

```
  sub topic t_hot floods messages ─┐
                                   ├─► each one gets changeInvisibleDuration(+20s)
                                   │    → invisible to the group for the next 20s
                                   ▼
  reader.receive() only returns currently-visible messages:
     ├─ t1, t2, t3 ...  normal messages (visible)   ← keep flowing
     └─ t_hot messages mostly invisible             ← suppressed, trickle back on expiry

  ⇒ the hot sub topic is throttled, the others are not starved = fairness
```

## 2. Consumption model

**Two modes.** A single `rocketmq.source.mode` option selects how the source subscribes and who
acknowledges:

- **SIMPLE** (default) — the source subscribes to one **normal** topic via a `SimpleConsumer` with
  an optional tag / SQL92 filter expression. Here the source itself acknowledges messages: it
  batches the receipt handles and acks them through that very consumer once the enclosing
  checkpoint completes (at-least-once).
- **LITE** (opt-in, requires `rocketmq.source.mode` = `LITE`) — the source binds one main lite topic
  via a `LiteSimpleConsumer` with a wildcard (generalized) subscription, so the broker funnels the
  messages of all sub topics through one receive stream. The source never acks: every emitted record
  carries a self-contained receipt handle, and the downstream operator takes the ack / throttle
  decision — which is what enables fair per-sub-topic throttling.

RocketMQ 5.x offers a push client (`PushConsumer`) and pull/Pop clients
(`SimpleConsumer` / `LiteSimpleConsumer`). Both connector modes build on the Pop clients
(available since `rocketmq-client-java` 5.2.1), whose consumption is split into three explicit
phases that map cleanly onto a Flink pipeline:

1. **Receive** — `receive(maxMessageNum, invisibleDuration)` long-polls the proxy; returned
   messages become *invisible* to the rest of the consumer group for `invisibleDuration`.
2. **Process** — the caller (here: the Flink job) processes the messages.
3. **Ack / changeInvisibleDuration** — `ack()` commits a message;
   `changeInvisibleDuration()` extends/renews its invisibility. **Un-acked messages are
   redelivered by the broker once their invisible duration expires** — this is the source of the
   at-least-once guarantee.

Key SDK facts the design relies on (verified against the SDK sources):

- **Wildcard subscription = `bindTopic` only.** Binding the main topic without calling
  `subscribeLite` puts the consumer into generalized mode: it receives from all sub topics.
  The consumer group must carry the broker-side attribute `lite.sub.wildcard=true`
  (a **consumer group** attribute — see Prerequisites).
- **Pop receipt handles are self-contained.** The server-side handle encodes its own routing
  (broker, queue, offsets, timestamps) as a string; the proxy routes an ack by the handle alone.
  Any consumer of the **same group** can ack a message, regardless of which consumer received it.
- **`SimpleConsumer`/`LiteSimpleConsumer` are thread-safe.** Multiple threads may block in
  `receive()` on one consumer instance concurrently.
- **`OffsetOption` (start-position control) exists only for exact `subscribeLite`**, therefore
  the wildcard-mode source has no startup-offset capability.

## 3. Architecture

### 3.1 Source reader

Because the broker performs **message-level load balancing** across the consumer group, the
source does not partition topics into splits. Every subtask binds the same main topic and simply
receives whatever the broker hands out; a single placeholder split only triggers the reader to
start. Per subtask:

```
subtask
 └─ SourceReader (mailbox thread)
     │   SIMPLE mode only: on notifyCheckpointComplete, ack the handles of that
     │   checkpoint through the SplitReader's consumer (see below)
     └─ SingleThreadFetcherManager → 1 fetcher thread
         └─ SplitReader
             ├─ 1 shared Pop consumer                  (thread-safe)
             │    SimpleConsumer in SIMPLE mode, LiteSimpleConsumer in LITE mode
             ├─ N ReceiveWorker threads                (N = rocketmq.source.fetch-concurrency)
             │    loop: consumer.receive(maxMessageNum, invisibleDuration)
             │    → put into a bounded in-memory queue (capacity = maxMessageNum × N)
             └─ fetch(): drain the queue in batches for the record emitter
```

- **One consumer, many workers.** A blocking `receive()` long poll only occupies its calling
  thread, so `fetch-concurrency` worker threads sharing one consumer keep that many long polls in
  flight — raising a single subtask's throughput without extra clientIds, heartbeats, connections
  or route caches. Total throughput scales with
  `parallelism × fetch-concurrency × maxMessageNum / receive-latency`.
- **SIMPLE-mode acks go through that same consumer.** The reader hands its completed receipt
  handles to `RocketMQGrpcSourceSplitReader#ack`, which issues the ack RPC on the consumer that
  received the messages. No second client, connection, clientId or credential resolution is
  involved, and no LITE-only client is used for normal topics. Pop consumers are thread-safe, so
  the mailbox thread may ack while the receive workers block in `receive()` on the same instance.
- **Backpressure = stop receiving.** When the bounded queue is full, workers block on `put()`;
  the pull model has no explicit pause API and does not need one.
- **Receive failures back off exponentially** (100 ms doubling up to 30 s) instead of
  hot-looping.

### 3.2 Receipt handle and record type

The source emits `AckableMessage<OUT> = { value, RocketMQReceiptHandle }`.

`RocketMQReceiptHandle` is a small, immutable, `Serializable` value object made of strings and an
int:

| Field group | Fields | Purpose |
| --- | --- | --- |
| Routing triple | `endpoint`, `namespace`, `consumerGroup` | Selects which pooled consumer must issue the ack RPC. Records from multiple sources (different clusters/namespaces/groups) can coexist in one stream. |
| Message identity | `topic`, `liteTopic`, `messageId`, `receiptHandle`, `deliveryAttempt` | Rebuilds the minimal SDK message view required by `ack` / `changeInvisibleDuration`. |

It deliberately contains **no credentials, no protobuf blob and no message body**, so it is safe
and cheap to ship across keyBy/shuffle boundaries. Dedicated `TypeInformation`/`TypeSerializer`
implementations (`AckableMessageTypeInfo` and the nested serializers) keep the type off the Kryo
fallback path and give it a stable wire format.

The single place that touches SDK-internal classes (`org.apache.rocketmq.client.java.*`) is
`RocketMQReceiptHandleCodec`: the public `apis` package exposes no receipt handle, so extracting
it on the source side and rebuilding a minimal message view on the ack side requires internal
types. Confining that (experimental) coupling to one `@Internal` adapter keeps the rest of the
connector SDK-clean.

### 3.3 Downstream acknowledgement (LITE mode)

This section applies to **LITE mode only**: it is the path where the ack decision is taken outside
the source, so the ack RPC cannot reuse the receiving consumer. (SIMPLE mode acks source-side
through the consumer that received the messages — see §3.1.) Downstream operators obtain a shared,
credential-free ack client:

- **Pooling.** `RocketMQAckClient` is shared per TaskManager JVM via reference-counted
  `acquire`/`release` keyed by the client configuration. Internally it lazily keeps one
  same-group `LiteSimpleConsumer` per routing triple `(endpoint, namespace, consumerGroup)`
  carried by the incoming handles. The consumer built for a handle uses the *handle's* endpoints
  and namespace (the SDK stamps ack requests with the consumer's namespace) and the *operator
  configuration's* credentials/TLS/timeout.
- **Retry & tolerance.** Ack RPCs retry with bounded exponential backoff. An
  `INVALID_RECEIPT_HANDLE` response (expired handle) is tolerated as a warning: the message will
  simply be redelivered, which is preferable to failing the job under at-least-once semantics.
- **Credentials never travel in the stream.** They are configured on the ack operator and
  resolved locally on each TaskManager — either static access/secret keys, or a pluggable
  per-endpoint `CredentialsResolver` (e.g. environment variables, mounted secrets, external KMS)
  that also keeps plaintext secrets out of the job graph.

Two wirings are provided on top of the client (DataStream API only):

1. **Client injection** — extend the abstract `RocketMQAckProcessFunction` and call the
   protected `ack()` / `changeInvisibleDuration()` from business logic. No extra edge in the job
   graph, lowest latency.
2. **Policy-based throttling** — implement `MessageThrottlePolicy<T>`
   (`Optional<Duration> onMessage(value)`: empty = ack, duration = defer) and run it with the
   one-stop `RocketMQThrottleProcessFunction<T>`.

### 3.4 Throttling semantics and safety valves

Using `changeInvisibleDuration` for throttling is a deliberate, documented trade-off: it is a
**per-message defer**, not a topic-level switch. Consequences and mitigations:

- Every deferred message costs one extra RPC, and deferring reorders the stream — downstream must
  already be idempotent and order-tolerant under at-least-once.
- Each redelivery increments `deliveryAttempt`; endless deferring would eventually push a message
  into the dead-letter queue. `RocketMQThrottleProcessFunction` therefore enforces two safety
  valves: once `deliveryAttempt` reaches `maxDeliveryAttempt` (default 16) the message is acked
  instead of deferred, and every requested delay is capped at `maxInvisibleDuration`
  (default 30 min).
- The broker slows down dispatch for lite topics with a large un-acked backlog; spreading load
  over many sub topics is part of the intended usage pattern.

### 3.5 Invisible-duration renewal

Under backpressure, messages can sit in the reader's internal queue long enough for their
invisible duration to expire, causing spurious redelivery. An optional
`InvisibleDurationRenewalPolicy` renews queue-resident messages `renewal-ahead-time` before
expiry. Messages are **frozen once emitted** to the record emitter: renewing refreshes the
receipt handle, which would invalidate the handle already travelling downstream.

## 4. Design decisions and alternatives

**Why downstream ack at all?** Mainstream Flink connectors with a Pop/lease-like model (GCP
Pub/Sub, Amazon SQS) ack **on the source side**, typically on `notifyCheckpointComplete`. That
only works when the ack decision is available in the source subtask. Here the requirement is to
decide *after* arbitrary downstream processing (possibly across shuffles), so the handle must
travel with the record and the ack must happen downstream.

Alternatives that were evaluated and rejected:

| Alternative | Why rejected |
| --- | --- |
| Source-side ack on checkpoint (Pub/Sub / SQS style) | Decision must be co-located with the source subtask; does not meet the requirement. |
| Feeding ack commands back to the reader | Flink's DAG is acyclic: DataStream Iterations are deprecated and checkpoint-incompatible, `OperatorCoordinator` does not connect different operators, and an external command topic would need credentials downstream anyway. |
| Carrying credentials in the record stream | Security red line. Credentials stay in operator configuration, resolved locally per TaskManager. |
| Serializing the SDK protobuf receipt blob | Unnecessary — the server-side handle string self-encodes routing; a handful of strings suffice. |

(The rejection of source-side checkpoint ack applies to the LITE requirement, where the ack decision
is only known downstream. SIMPLE mode has no such decision and intentionally adopts that very
approach — see §5.)

Other decisions:

- **Commit point is user-driven.** The connector does not align acks with checkpoints; the user
  acks when processing is complete. This gives at-least-once with user-controlled granularity.
- **One main topic per source** (SDK `bindTopic` limitation). Use multiple sources for multiple
  main topics; the routing triple in the handle lets one downstream ack operator serve all of
  them.
- **Ack/throttle APIs are DataStream-only.** The SQL/Table path consumes values only.

## 5. Delivery semantics

Both modes provide **at-least-once**: un-acked messages are redelivered by the broker once their
`invisible-duration` expires, so downstream processing must be idempotent. What differs is *who*
acks and *when*.

### LITE mode

- The source never acks; the ack decision is deferred to downstream operators, which gives
  at-least-once with user-controlled granularity.
- `invisible-duration` must cover the full downstream processing time of a message (including
  shuffles and slow operators such as model inference); use the renewal policy when queueing
  time is unpredictable.
- Exactly-once is out of scope for the Pop model: there is no offset the connector could
  checkpoint-align, and acks are user-driven by design.

### SIMPLE mode

- Once a message is emitted, its receipt handle is registered with a checkpoint-aligned tracker;
  the handle is snapshotted together with the enclosing checkpoint and the source batch-acks all
  handles of a checkpoint from `notifyCheckpointComplete` after that checkpoint completes.
- The acks are issued **through the consumer that received the messages** (the split reader's
  `SimpleConsumer`), not through the pooled `RocketMQAckClient` of §3.3. The reader therefore opens
  no second gRPC connection, resolves no credentials of its own, and never routes normal-topic acks
  through a LITE client.
- On a failure before completion the pending handles are simply dropped; the un-acked messages are
  redelivered by the broker after their `invisible-duration` expires. That redelivery *is* the
  at-least-once guarantee, which is why the tracker holds no failure-survivable state.
- **Checkpointing is required.** Without it `notifyCheckpointComplete` never fires, nothing is ever
  acked, and every message is redelivered forever.
- **Size `invisible-duration` with headroom — do not compute it as a tight bound.** The invisible
  window is consumed by more than one checkpoint cycle:

  | Consumer of the window | Why it counts |
  | --- | --- |
  | Queueing before emission | The window starts when the broker serves `receive()`, not when the record is emitted. Time spent in the reader's internal queue (`max-message-num` × `fetch-concurrency` slots) and in deserialization is already gone. |
  | Waiting for the next barrier | A record emitted just after a barrier waits almost a full checkpoint interval for the next one. |
  | Completing that checkpoint | Barrier alignment plus state materialisation, bounded by the checkpoint timeout. |
  | **A failed or timed-out checkpoint** | The tracker only clears handles for checkpoints that actually complete, so handles of a failed checkpoint wait for the next successful one — roughly **another full interval**, more if failures repeat. |
  | Draining the ack batch | `notifyCheckpointComplete` acks the batch one handle per RPC, so the last handles of a large batch wait for the earlier ones. |

  A practical starting point is therefore

  ```
  invisible-duration ≥ 2 × checkpoint interval + checkpoint timeout + headroom
  ```

  Configuring it as merely `interval + timeout` still satisfies at-least-once, but a single failed
  checkpoint is then enough to make the batch visible again and reprocessed.
- Emitted-but-not-yet-acked handles are **not renewed**. The SDK's `changeInvisibleDuration` does
  not return the new receipt handle, so the source cannot refresh a handle it has already handed to
  the tracker; sizing `invisible-duration` as above is the substitute. A renewal policy (§7) only
  covers the *first* row of the table above — messages still queued inside the reader — because the
  reader freezes a handle the moment the record is emitted.

---

## 6. Prerequisites

The default SIMPLE mode only needs an ordinary RocketMQ 5.x topic plus a consumer group. The
following applies to the opt-in LITE mode:

- RocketMQ 5.x cluster with lite topic support enabled on the broker
  (`enableMultiDispatch`, `enableLmq`, etc. — preset by the official Helm chart).
- A LITE-type main topic and a consumer group with the wildcard attribute:

```bash
mqadmin updateTopic -n <ns:9876> -c <cluster> -t LiteMainTopic -a "+message.type=LITE"
mqadmin updateSubGroup -n <ns:9876> -c <cluster> -g GID-lite \
    --attributes "+lite.sub.wildcard=true"
```

`lite.sub.wildcard` is a **consumer group** attribute, not a topic attribute. Without it the
wildcard subscription receives nothing.

## 7. Source

The source runs in **SIMPLE** mode unless another mode is selected, i.e. it subscribes to a normal
topic and acks the messages itself once the enclosing checkpoint completes (checkpointing must be
enabled):

```java
RocketMQGrpcSource<String> source = RocketMQGrpcSource.<String>builder()
        .setEndpoints("127.0.0.1:8081")
        .setConsumerGroup("GID-example")
        .setTopic("normal-topic")
        .setFilterExpression("tagA||tagB")
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

DataStream<AckableMessage<String>> stream =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "grpc-source");
```

The output type is `AckableMessage<OUT>`: the payload plus a serializable
`RocketMQReceiptHandle` (endpoint, namespace, consumerGroup, lite topic, receipt handle). The
handle can cross keyBy/shuffle boundaries; credentials never travel with it. In SIMPLE mode the
handle is acknowledged automatically by the source once the enclosing checkpoint completes, so
downstream neither needs nor should invoke the ack / throttle operators.

### LITE mode

LITE mode is opt-in: select it explicitly and bind a main lite topic instead of a normal topic. The
source then never acks by itself and the downstream operators take the ack / throttle decision.

```java
RocketMQGrpcSource<String> source = RocketMQGrpcSource.<String>builder()
        .setEndpoints("127.0.0.1:8081")
        .setConsumerGroup("GID-lite")
        .setMode(ConsumerMode.LITE)
        .setMainTopic("LiteMainTopic")
        .setFetchConcurrency(4)
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();
```

### Downstream ack / throttling

The ack / throttle wirings below apply to LITE mode only; in SIMPLE mode the source acks the
messages itself.

Unacked messages are redelivered by the broker after `invisible-duration` expires
(at-least-once — downstream must be idempotent). Two wirings are provided:

1. **Client injection** — extend `RocketMQAckProcessFunction` and call `ack()` /
   `changeInvisibleDuration()` directly.
2. **Policy-based throttling** — implement `MessageThrottlePolicy<T>`
   (`Optional<Duration> onMessage(value)`: empty = ack, duration = defer via
   `changeInvisibleDuration`) and run it with `RocketMQThrottleProcessFunction<T>`.
   Built-in safety valves: maxDeliveryAttempt=16, maxInvisibleDuration=30min (avoids DLQ from
   repeated defers).

Ack clients are pooled per TaskManager keyed by (endpoint, namespace, consumerGroup) with
reference counting; ack RPCs retry with bounded exponential backoff, and
`INVALID_RECEIPT_HANDLE` is tolerated as a warning (the message will simply be redelivered).

### Invisible-duration renewal

Under backpressure, messages waiting in the reader queue may exceed `invisible-duration` and get
redelivered. Configure a renewal policy to extend the invisible time before it expires:

```java
builder.setRenewalPolicyClass("com.example.MyRenewalPolicy")
       .setRenewalAheadTime(Duration.ofSeconds(5));
```

Renewal only covers records that are **still queued inside the reader**: the reader freezes a
handle the moment the record is emitted, because renewing would invalidate the handle that has
already travelled downstream. Consequently it does not extend the window for a record that is
waiting for its downstream ack (LITE) or for its checkpoint to complete (SIMPLE) — in SIMPLE mode a
renewal policy is therefore no substitute for sizing `invisible-duration` as described in §5.

## 8. Sink

```java
RocketMQGrpcSink<String> sink = RocketMQGrpcSink.<String>builder()
        .setEndpoints("127.0.0.1:8081")
        .setTopic("LiteMainTopic")
        .setLiteTopic("my-lite-topic")
        .setValueOnlySerializer(new SimpleStringSchema())
        .build();
```

## 9. Options

### Client (`rocketmq.client.*`)

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `rocketmq.client.endpoints` | String | (none) | gRPC proxy endpoints, required |
| `rocketmq.client.namespace` | String | `""` | Instance namespace |
| `rocketmq.client.access-key` / `rocketmq.client.secret-key` | String | (none) | ACL credentials (skipped when unset) |
| `rocketmq.client.credentials-resolver-class` | String | (none) | Pluggable per-endpoint `CredentialsResolver` |
| `rocketmq.client.tls-enabled` | Boolean | false | TLS transport |
| `rocketmq.client.request-timeout` | Duration | 3s | gRPC request timeout |

### Source (`rocketmq.source.*`)

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `rocketmq.source.mode` | ConsumerMode | `SIMPLE` | Consumption mode: `SIMPLE` (default; normal topic, checkpoint-aligned source ack) or `LITE` (main lite topic, downstream ack) |
| `rocketmq.source.main-topic` | String | (none) | Main lite topic to bind, required in LITE mode |
| `rocketmq.source.topic` | String | (none) | Normal topic to subscribe, required in SIMPLE mode |
| `rocketmq.source.filter-expression` | String | (none) | SIMPLE-mode tag / SQL92 filter expression (absent = receive all messages) |
| `rocketmq.source.filter-type` | String | `TAG` | Type of `filter-expression`: `TAG` or `SQL92` |
| `rocketmq.source.consumer-group` | String | (none) | Consumer group, required |
| `rocketmq.source.fetch-concurrency` | Integer | 1 | Concurrent long-polling receive workers |
| `rocketmq.source.await-duration` | Duration | 20s | Long-polling await time |
| `rocketmq.source.invisible-duration` | Duration | 60s | Invisible time per receive (min 10s); size it with headroom — see §5 |
| `rocketmq.source.max-message-num` | Integer | 32 | Max messages per receive |
| `rocketmq.source.renewal-policy-class` | String | (none) | `InvisibleDurationRenewalPolicy` implementation |
| `rocketmq.source.renewal-ahead-time` | Duration | 5s | Renew this long before invisible expiry |

### Sink (`rocketmq.sink.*`)

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `rocketmq.sink.topic` | String | (none) | Main lite topic, required |
| `rocketmq.sink.lite-topic` | String | (none) | Lite topic within the main topic |
| `rocketmq.sink.max-attempts` | Integer | 3 | Producer send attempts |

## 10. SQL

The SQL connector identifier is `rocketmq-grpc` (fat-jar module
`flink-sql-connector-rocketmq-grpc`). It exposes the same option keys as above, plus
`rocketmq.source.fetch-concurrency`. The SQL/Table path consumes message values only; the
downstream ack / throttling APIs are DataStream-only.

The SQL source runs in `simple` mode by default (`source.mode` defaults to `simple`), i.e. the
configured `topic` is consumed as a normal topic and the source acks on checkpoint completion, so
checkpointing must be enabled:

```sql
CREATE TABLE orders (
    f0 STRING
) WITH (
    'connector' = 'rocketmq-grpc',
    'endpoints' = '127.0.0.1:8081',
    'topic' = 'normal-topic',
    'source.consumer-group' = 'GID-example',
    'source.filter-expression' = 'tagA || tagB',
    'format' = 'json'
);
```

To consume a main lite topic instead, opt into `lite` mode explicitly with
`'source.mode' = 'lite'`; the `topic` option is then bound as the main lite topic and the SQL path
performs no acknowledgement (the ack / throttling APIs are DataStream-only).

## 11. Known limitations

- `LiteSimpleConsumer` is experimental in the SDK (5.2.1+) and offers synchronous APIs only.
- SIMPLE mode requires checkpointing; without checkpoints messages are never acknowledged and are
  redelivered indefinitely.
- SIMPLE mode has no way to renew a handle it has already emitted, so an `invisible-duration` sized
  as a tight bound turns any failed checkpoint into reprocessed messages (see §5).
- One main lite topic per source; no startup-offset control in wildcard-subscription mode
  (`OffsetOption` is limited to exact `subscribeLite`).
- Throttling defers messages per message (extra RPC per defer, reordering, `deliveryAttempt`
  growth); rely on the built-in safety valves and idempotent downstream processing.
- The broker throttles dispatch for lite topics with large un-acked backlogs; design the
  workload to spread across sub topics.
