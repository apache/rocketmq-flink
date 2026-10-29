# Remoting Connector (DataStream API)

The remoting connector (`flink-connector-rocketmq`) talks to RocketMQ over the classic remoting
protocol using `rocketmq-client`. It provides a FLIP-27 `RocketMQSource` and a SinkV2
`RocketMQSink` with two-phase-commit support.

## Source

```java
RocketMQSource<String> source = RocketMQSource.<String>builder()
        .setEndpoints("127.0.0.1:9876")
        .setGroupId("GID-flink")
        .setTopics("topic-a", "topic-b")
        .setMinOffsets(OffsetsSelector.latest())      // starting offsets
        .setBodyOnlyDeserializer(new SimpleStringSchema())
        .build();
```

Builder methods:

| Method | Description |
| --- | --- |
| `setEndpoints(String)` | Name server address, required |
| `setGroupId(String)` | Consumer group, required |
| `setTopics(String... / List<String>)` | Topics to consume, required |
| `setMinOffsets(OffsetsSelector)` | Starting offsets: `earliest()` / `latest()` / `committedOffsets()` / `timestamp(ts)` |
| `setBounded(OffsetsSelector)` / `setUnbounded(OffsetsSelector)` | Stopping offsets and boundedness |
| `setDeserializer(...)` / `setBodyOnlyDeserializer(...)` | Record deserialization |
| `setConfig(Configuration)` / `setProperties(Properties)` | Pass any option from the tables below |

Notes:

- The source consumes with `DefaultLitePullConsumer.assign()`; with checkpointing enabled offsets
  are committed on checkpoint (`rocketmq.source.offset.commit.checkpoint`, default true).
- On Aliyun commercial 5.x instances the admin route lookup is not available; the source falls
  back to consumer-side offset lookup automatically. Topics and groups must be created in the
  console (mqadmin is not supported there).

## Sink

```java
RocketMQSink<String> sink = RocketMQSink.<String>builder()
        .setEndpoints("127.0.0.1:9876")
        .setGroupId("PID-flink")
        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)   // or AT_LEAST_ONCE / NONE
        .setSerializer(mySerializationSchema)                   // builds Message objects (topic/tag/keys per record)
        .build();
```

- `AT_LEAST_ONCE` uses async sends with a pending counter and a flush barrier on checkpoint.
- `EXACTLY_ONCE` uses `TransactionMQProducer` two-phase commit driven by Flink checkpoints;
  set `rocketmq.sink.transaction.timeout` larger than the checkpoint interval.

> **`EXACTLY_ONCE` is NOT supported when the endpoints point at a proxy** (Alibaba Cloud
> RocketMQ 5.x commercial instances, or any deployment where clients reach brokers through a
> proxy). Use `AT_LEAST_ONCE` with idempotent downstream consumers in those deployments.
>
> Why: a proxy keeps the transaction state of a half message only in the memory of the proxy
> process that received it, and drops it after `transactionDataExpireMillis` (30 seconds by
> default). Flink commits at the end of a checkpoint, so a commit that lands on a different
> proxy pod, or arrives after the TTL, finds no state; the proxy then maps
> `TRANSACTION_DATA_NOT_FOUND` to a **success** response and drops it without a single log
> line — the job sees successful commits while the records never become visible, and nothing
> surfaces anywhere. The broker-side transaction check does not rescue these half messages
> through the proxy path either. The failure is silent by construction, so it cannot be
> mitigated by tuning the checkpoint interval.
>
> `EXACTLY_ONCE` **is supported on direct deployments** (endpoints = nameserver addresses, the
> connector talks to brokers directly): the transaction state lives on the broker that holds
> the half message and is persisted, so commits succeed regardless of the checkpoint interval.

## Client options (`rocketmq.client.*`)

Shared by source and sink.

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `rocketmq.client.endpoints` | String | (none) | Name server address, required |
| `rocketmq.client.namespace` | String | (none) | Instance namespace |
| `rocketmq.client.accessKey` / `rocketmq.client.secretKey` | String | (none) | ACL credentials |
| `rocketmq.client.channel` | Enum | `CLOUD` | Access channel (`LOCAL` / `CLOUD`) |
| `rocketmq.client.tls.enable` | Boolean | false | TLS transport |
| `rocketmq.client.network.timeout.ms` | Long | 30000 | Client API timeout |
| `rocketmq.client.callback.threads` | Integer | CPU cores | Client callback executor threads |
| `rocketmq.client.partition.discovery.interval.ms` | Long | 10000 | Route/partition discovery interval |
| `rocketmq.client.poll.timeout` | Long | 1000 | How long a source fetch blocks while no record is available. Returns as soon as records arrive, so it costs no data-path latency; it also bounds how long a fetch can outlive a close request |
| `rocketmq.client.unitMode` / `rocketmq.client.unitName` | Boolean / String | false / (none) | Unit mode routing |
| `rocketmq.client.debug` | Boolean | false | Verbose client logs |

## Source options (`rocketmq.source.*`)

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `rocketmq.source.topic` | String | (none) | Topic, required |
| `rocketmq.source.group` | String | (none) | Consumer group, required |
| `rocketmq.source.filter.tag` | String | `*` | Tag filter, `tagA\|\|tagB` for several. Evaluated on the client |
| `rocketmq.source.filter.sql` | String | (none) | SQL92 filter over message properties. Evaluated on the client with the same `rocketmq-filter` implementation the broker uses, so predicate semantics match server side filtering. An expression that does not compile fails when the reader is built |
| `rocketmq.source.startup.scan.mode` | String | `latest` | `earliest` / `latest` / `timestamp` / `committed`. Any other value is rejected |
| `rocketmq.source.startup.offset.timestamp` | Long | (none) | Startup timestamp; auto-infers `scan.mode=timestamp` |
| `rocketmq.source.allocate.strategy` | String | `consistent-hash` | Queue-to-subtask allocation strategy |
| `rocketmq.source.pull.threads.num` | Integer | 20 | Pull thread pool size |
| `rocketmq.source.pull.batch.size` | Long | 32 | Max messages per pull |
| `rocketmq.source.pull.threshold.queue` | Long | 1000 | Queue-level flow control threshold |
| `rocketmq.source.pull.threshold.all` | Long | 10000 | Total flow control threshold |
| `rocketmq.source.pull.rpc.timeout` | Long | 20000 | Pull RPC timeout (ms) |
| `rocketmq.source.pull.rpc.exception.delay` | Long | 3000 | Pull retry delay on exception (ms) |
| `rocketmq.source.offset.commit.auto` | Boolean | false | Let the client commit the offset of everything it polled on its own timer. Off by default: Flink commits the offsets of checkpointed state instead, and broker side offsets never decide where a restored job starts reading, so this only changes what consumer progress monitoring reports |
| `rocketmq.source.offset.commit.interval` | Long | 5000 | Interval of the client side auto commit (ms) |
| `rocketmq.source.offset.commit.checkpoint` | Boolean | true | Commit offsets on checkpoint |
| `rocketmq.source.trace.enable` | Boolean | true | Message trace |
| `rocketmq.source.trace.topic` | String | (none) | Customized trace topic |

## Source metrics

Reported per message queue under `RocketmqSourceReader`, keyed by `topic` and
`queue`:

| Metric | Type | Description |
| --- | --- | --- |
| `currentOffset` | Gauge | Offset the reader has consumed up to |
| `committedOffset` | Gauge | Offset last committed to the broker |
| `records-lag` | Gauge | Records the broker still holds beyond `currentOffset` |
| `commitsSucceeded` / `commitsFailed` | Counter | Offset commit results, under the reader group |
| `bytes-consumed-total` | Counter | Under `RocketMQConsumer.consumer-fetch-manager-metrics` |

The lag of every assigned queue is also summed into Flink's standard
`pendingRecords`. Lag needs the broker's maximum offset, so the reader refreshes
it asynchronously at most once every ten seconds; reading a metric never
triggers a remote call.

A queue with a positive `records-lag` and no records being emitted means the
broker is not delivering to this consumer group, which is how an unregistered
group or a missing subscription relationship shows up.

## Sink options (`rocketmq.sink.*`)

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `rocketmq.sink.topic` | String | (none) | Topic, required for SQL |
| `rocketmq.sink.group` | String | `PID-flink-producer` | Producer group |
| `rocketmq.sink.tag` | String | (none) | Static message tag |
| `rocketmq.sink.delivery.guarantee` | String | `AT_LEAST_ONCE` | `NONE` / `AT_LEAST_ONCE` / `EXACTLY_ONCE` (`EXACTLY_ONCE` is **not supported via a proxy endpoint**, see the delivery-semantics note above) |
| `rocketmq.sink.transaction.timeout` | Long | 900 (s) | Transaction timeout for EXACTLY_ONCE |
| `rocketmq.sink.send.timeout` | Long | 5000 | Send timeout (ms) |
| `rocketmq.sink.send.retry.times` | Integer | 3 | Send retry times |
| `rocketmq.sink.send.pending.max` | Integer | 1000 | Max in-flight async sends (AT_LEAST_ONCE) |
| `rocketmq.sink.executor.num` | Integer | 4 | Producer executor threads |
| `rocketmq.sink.key.columns` | String | (none) | SQL: columns used as message keys |
| `rocketmq.sink.tag.dynamic.enable` / `rocketmq.sink.tag.dynamic.column` | Boolean / String | false / (none) | SQL: derive the tag from a column |
