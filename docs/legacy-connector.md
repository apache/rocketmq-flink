# Legacy Connector (Deprecated)

> The legacy `RocketMQSourceFunction` / `RocketMQSink` are based on the deprecated Flink
> `SourceFunction` / `SinkFunction` APIs. New jobs should use the FLIP-27 `RocketMQSource` and
> SinkV2 `RocketMQSink` documented in [remoting-connector.md](remoting-connector.md). This page
> is kept for existing jobs only.

The legacy classes live under `org.apache.flink.streaming.connectors.rocketmq`.

## RocketMQSourceFunction

Construct it with a `KeyValueDeserializationSchema` and a `Properties` holding RocketMQ configs:

```java
RocketMQSourceFunction<Map<Object, Object>> source = new RocketMQSourceFunction<>(
        new SimpleKeyValueDeserializationSchema("id", "address"), consumerProps);
source.setStartFromGroupOffsets(OffsetResetStrategy.LATEST);
env.addSource(source);
```

Based on the RocketMQ pull consumer; provides exactly-once when checkpointing is enabled,
otherwise no reliability guarantee.

```java
public interface KeyValueDeserializationSchema<T> extends ResultTypeQueryable<T>, Serializable {
    T deserializeKeyAndValue(byte[] key, byte[] value);
}
```

### Startup policies

| Method | Description |
| --- | --- |
| `setStartFromEarliest()` | Consume from the earliest offset (no-state restart) |
| `setStartFromLatest()` | Consume from the latest offset (no-state restart) |
| `setStartFromTimeStamp(ts)` | Consume from the closest timestamp in each queue |
| `setStartFromGroupOffsets(OffsetResetStrategy)` | Committed offset if present, else LATEST/EARLIEST fallback |
| `setStartFromSpecificOffsets(Map<MessageQueue, Long>)` | Explicit per-queue offsets; unspecified queues use group offsets |

These policies only take effect when the job starts without state. When recovering from a
checkpoint, offsets are restored from the checkpointed state.

## RocketMQSink (SinkFunction)

Construct it with a `Properties`; provides at-least-once when checkpointing is enabled and
`withBatchFlushOnCheckpoint(true)` is set. Otherwise reliability depends on the producer retry
policy (sync by default; `withAsync(true)` switches to async).

```java
stream.addSink(new RocketMQSink(producerProps).withBatchFlushOnCheckpoint(true));
```

Per-record topic/tag selection uses `TopicSelector`, serialization uses
`KeyValueSerializationSchema` (`SimpleKeyValueSerializationSchema`, `DefaultTopicSelector` and
`SimpleTopicSelector` are provided):

```java
public interface KeyValueSerializationSchema<T> extends Serializable {
    byte[] serializeKey(T tuple);
    byte[] serializeValue(T tuple);
}

public interface TopicSelector<T> extends Serializable {
    String getTopic(T tuple);
    String getTag(T tuple);
}
```

## RocketMQConfig properties

### Common / producer

| Name | Description | Default |
| --- | --- | --- |
| `nameserver.address` | Name server address (required) | null |
| `nameserver.poll.interval` | Name server poll interval (ms) | 30000 |
| `brokerserver.heartbeat.interval` | Broker heartbeat interval (ms) | 30000 |
| `producer.group` | Producer group | random UUID |
| `producer.retry.times` | Send retry times | 3 |
| `producer.timeout` | Send timeout (ms) | 3000 |

### Consumer

| Name | Description | Default |
| --- | --- | --- |
| `consumer.group` | Consumer group (required) | null |
| `consumer.topic` | Topic (required) | null |
| `consumer.tag` | Tag filter | `*` |
| `consumer.offset.persist.interval` | Auto commit offset interval (ms) | 5000 |
| `consumer.pull.thread.pool.size` | Pull thread pool size | 20 |
| `consumer.batch.size` | Pull batch size | 32 |
| `consumer.delay.when.message.not.found` | Delay when no message found (ms) | 10 |
