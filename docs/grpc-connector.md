# gRPC Connector (LiteSimpleConsumer)

The gRPC connector (`flink-connector-rocketmq-grpc`) targets RocketMQ 5.x **lite topics** using
`rocketmq-client-java` 5.2.1+. The source binds one main lite topic with a wildcard subscription,
never acks by itself, and hands the ack / throttle decision to downstream operators.

## Prerequisites

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

## Source

```java
RocketMQGrpcSource<String> source = RocketMQGrpcSource.<String>builder()
        .setEndpoints("127.0.0.1:8081")
        .setConsumerGroup("GID-lite")
        .setMainTopic("LiteMainTopic")
        .setFetchConcurrency(4)
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

DataStream<AckableMessage<String>> stream =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "grpc-source");
```

The output type is `AckableMessage<OUT>`: the payload plus a serializable
`RocketMQReceiptHandle` (endpoint, namespace, consumerGroup, lite topic, receipt handle). The
handle can cross keyBy/shuffle boundaries; credentials never travel with it.

### Downstream ack / throttling

Unacked messages are redelivered by the broker after `invisible-duration` expires
(at-least-once — downstream must be idempotent). Three wirings are provided:

1. **Standalone ack operator** — user operators emit `AckCommand{handle, action, delay}`,
   `RocketMQAckOperator` executes them. Users never touch the SDK.
2. **Client injection** — extend `RocketMQAckProcessFunction` and call `ack()` /
   `changeInvisibleDuration()` directly.
3. **Policy-based throttling** — implement `MessageThrottlePolicy<T>`
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

## Sink

```java
RocketMQGrpcSink<String> sink = RocketMQGrpcSink.<String>builder()
        .setEndpoints("127.0.0.1:8081")
        .setTopic("LiteMainTopic")
        .setLiteTopic("my-lite-topic")
        .setValueOnlySerializer(new SimpleStringSchema())
        .build();
```

## Options

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
| `rocketmq.source.main-topic` | String | (none) | Main lite topic to bind, required |
| `rocketmq.source.consumer-group` | String | (none) | Consumer group, required |
| `rocketmq.source.fetch-concurrency` | Integer | 1 | Concurrent long-polling receive workers |
| `rocketmq.source.await-duration` | Duration | 30s | Long-polling await time |
| `rocketmq.source.invisible-duration` | Duration | 60s | Invisible time per receive (min 10s) |
| `rocketmq.source.max-message-num` | Integer | 32 | Max messages per receive |
| `rocketmq.source.renewal-policy-class` | String | (none) | `InvisibleDurationRenewalPolicy` implementation |
| `rocketmq.source.renewal-ahead-time` | Duration | 5s | Renew this long before invisible expiry |

### Sink (`rocketmq.sink.*`)

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `rocketmq.sink.topic` | String | (none) | Main lite topic, required |
| `rocketmq.sink.lite-topic` | String | (none) | Lite topic within the main topic |
| `rocketmq.sink.max-attempts` | Integer | 3 | Producer send attempts |

## SQL

The SQL connector identifier is `rocketmq-grpc` (fat-jar module
`flink-sql-connector-rocketmq-grpc`). It exposes the same option keys as above, plus
`rocketmq.source.fetch-concurrency`.
