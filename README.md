# RocketMQ-Flink
[![Build Status](https://app.travis-ci.com/apache/rocketmq-flink.svg?branch=main)](https://app.travis-ci.com/apache/rocketmq-flink) [![Coverage Status](https://coveralls.io/repos/github/apache/rocketmq-flink/badge.svg?branch=main)](https://coveralls.io/github/apache/rocketmq-flink?branch=main)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Average time to resolve an issue](http://isitmaintained.com/badge/resolution/apache/rocketmq-flink.svg)](http://isitmaintained.com/project/apache/rocketmq-flink "Average time to resolve an issue")
[![Percentage of issues still open](http://isitmaintained.com/badge/open/apache/rocketmq-flink.svg)](http://isitmaintained.com/project/apache/rocketmq-flink "Percentage of issues still open")
[![Twitter Follow](https://img.shields.io/twitter/follow/ApacheRocketMQ?style=social)](https://twitter.com/intent/follow?screen_name=ApacheRocketMQ)

RocketMQ integration for [Apache Flink](https://flink.apache.org/). This module includes the RocketMQ source and sink that allows a flink job to either write messages into a topic or read from topics in a flink job.

## RocketMQSourceFunction
To use the `RocketMQSourceFunction`,  you construct an instance of it by specifying a KeyValueDeserializationSchema instance and a Properties instance which including rocketmq configs.
`RocketMQSourceFunction(KeyValueDeserializationSchema<OUT> schema, Properties props)`
The RocketMQSourceFunction is based on RocketMQ pull consumer mode, and provides exactly once reliability guarantees when checkpoints are enabled.
Otherwise, the source doesn't provide any reliability guarantees.

### KeyValueDeserializationSchema
The main API for deserializing topic and tags is the `org.apache.rocketmq.flink.legacy.common.serialization.KeyValueDeserializationSchema` interface.
`rocketmq-flink` includes general purpose `KeyValueDeserializationSchema` implementations called `SimpleKeyValueDeserializationSchema`.

```java
public interface KeyValueDeserializationSchema<T> extends ResultTypeQueryable<T>, Serializable {
    T deserializeKeyAndValue(byte[] key, byte[] value);
}
```

## RocketMQSink
To use the `RocketMQSink`,  you construct an instance of it by specifying KeyValueSerializationSchema & TopicSelector instances and a Properties instance which including rocketmq configs.
`RocketMQSink(KeyValueSerializationSchema<IN> schema, TopicSelector<IN> topicSelector, Properties props)`
The RocketMQSink provides at-least-once reliability guarantees when checkpoints are enabled and `withBatchFlushOnCheckpoint(true)` is set.
Otherwise, the sink reliability guarantees depends on rocketmq producer's retry policy, for this case, the messages sending way is sync by default,
but you can change it by invoking `withAsync(true)`. 

### KeyValueSerializationSchema
The main API for serializing topic and tags is the `org.apache.rocketmq.flink.legacy.common.serialization.KeyValueSerializationSchema` interface.
`rocketmq-flink` includes general purpose `KeyValueSerializationSchema` implementations called `SimpleKeyValueSerializationSchema`.

```java
public interface KeyValueSerializationSchema<T> extends Serializable {

    byte[] serializeKey(T tuple);

    byte[] serializeValue(T tuple);
}
```

### TopicSelector
The main API for selecting topic and tags is the `org.apache.rocketmq.flink.legacy.common.selector.TopicSelector` interface.
`rocketmq-flink` includes general purpose `TopicSelector` implementations called `DefaultTopicSelector` and `SimpleTopicSelector`.

```java
public interface TopicSelector<T> extends Serializable {

    String getTopic(T tuple);

    String getTag(T tuple);
}
```

## Examples
The following is an example which receive messages from RocketMQ brokers and send messages to broker after processing.

 ```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(3000);

        Properties consumerProps = new Properties();
        consumerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, "localhost:9876");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_GROUP, "c002");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_TOPIC, "flink-source2");

        Properties producerProps = new Properties();
        producerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, "localhost:9876");

        RocketMQSourceFunction<Map<Object,Object>> source = new RocketMQSourceFunction(
                new SimpleKeyValueDeserializationSchema("id", "address"), consumerProps);
        // use group offsets.
        // If there is no committed offset,consumer would start from the latest offset.
        source.setStartFromGroupOffsets(OffsetResetStrategy.LATEST);
        env.addSource(source)
            .name("rocketmq-source")
            .setParallelism(2)
            .process(new ProcessFunction<Map<Object, Object>, Map<Object, Object>>() {
                @Override
                public void processElement(
                        Map<Object, Object> in,
                        Context ctx,
                        Collector<Map<Object, Object>> out) {
                    HashMap result = new HashMap();
                    result.put("id", in.get("id"));
                    String[] arr = in.get("address").toString().split("\\s+");
                    result.put("province", arr[arr.length - 1]);
                    out.collect(result);
                }
            })
            .name("upper-processor")
            .setParallelism(2)
            .process(new ProcessFunction<Map<Object, Object>, Message>() {
                @Override
                public void processElement(Map<Object, Object> value, Context ctx, Collector<Message> out) {
                    String jsonString = JSONObject.toJSONString(value);
                    Message message =
                            new Message(
                                    "flink-sink2",
                                    "",
                                    jsonString.getBytes(StandardCharsets.UTF_8));
                    out.collect(message);
                }
            })
            .addSink(new RocketMQSink(producerProps).withBatchFlushOnCheckpoint(true))
            .name("rocketmq-sink")
            .setParallelism(2);

        try {
            env.execute("rocketmq-flink-example");
        } catch (Exception e) {
            e.printStackTrace();
        }
 ```

## Configurations
The following configurations are all from the class `org.apache.rocketmq.flink.legacy.RocketMQConfig`.

### Producer Configurations
| NAME        | DESCRIPTION           | DEFAULT  |
| ------------- |:-------------:|:------:|
| nameserver.address      | name server address *Required* | null |
| nameserver.poll.interval      | name server poll topic info interval     |   30000 |
| brokerserver.heartbeat.interval | broker server heartbeat interval      |    30000 |
| producer.group | producer group      |    `UUID.randomUUID().toString()` |
| producer.retry.times | producer send messages retry times      |    3 |
| producer.timeout | producer send messages timeout      |    3000 |


### Consumer Configurations
| NAME        | DESCRIPTION           | DEFAULT  |
| ------------- |:-------------:|:------:|
| nameserver.address      | name server address *Required* | null |
| nameserver.poll.interval      | name server poll topic info interval     |   30000 |
| brokerserver.heartbeat.interval | broker server heartbeat interval      |    30000 |
| consumer.group | consumer group *Required*     |    null |
| consumer.topic | consumer topic *Required*       |    null |
| consumer.tag | consumer topic tag      |    * |
| consumer.offset.persist.interval | auto commit offset interval      |    5000 |
| consumer.pull.thread.pool.size | consumer pull thread pool size      |    20 |
| consumer.batch.size | consumer messages batch size      |    32 |
| consumer.delay.when.message.not.found | the delay time when messages were not found      |    10 |

### Consumer Strategy

```java
RocketMQSourceFunction<String> source = new RocketMQSourceFunction<>(
        new SimpleStringDeserializationSchema(), props);
HashMap<MessageQueue, Long> brokerMap = new HashMap<>();
brokerMap.put(new MessageQueue("tp_driver_tag_sync_back", "broker-a", 1), 201L);
brokerMap.put(new MessageQueue("tp_driver_tag_sync_back", "broker-c", 3), 123L);
source.setStartFromSpecificOffsets(brokerMap);
```
RocketMQSourceFunction offer five initialization policies 
* setStartFromEarliest
* setStartFromLatest
* setStartFromTimeStamp with timestamp
* setStartFromGroupOffsets with `OffsetResetStrategy`
* setStartFromSpecificOffsets

| STRATEGY                    | DESCRIPTION                                                  |
| --------------------------- | ------------------------------------------------------------ |
| EARLIEST                    | consume from the earliest offset after restart with no state |
| LATEST                      | consume from the latest offset after restart with no state   |
| TIMESTAMP                   | consume from the closest timestamp of data in each broker's queue |
| GROUP_OFFSETS with LATEST   | If broker has the committed offset then consume from the next else consume from the latest offset |
| GROUP_OFFSETS with EARLIEST | If broker has the committed offset ,consume from the next ,otherwise consume from the earlist offset.It's useful when server  expand  broker |
| SPECIFIC_OFFSETS            | consumer from the specificOffsets in broker's queues.Group offsets will be returned from those broker's queues whose didn't be specified |

**Attention**

Only if Flink job starts with none state, these strategies are effective. If the job recovers from the checkpoint, the offset would intialize from the stored data.

## RocketMQ SQL Connector

### How to create a RocketMQ table

The example below shows how to create a RocketMQ table:

```sql
CREATE TABLE rocketmq_source (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING
) WITH (
  'connector' = 'rocketmq',
  'topic' = 'user_behavior',
  'consumerGroup' = 'behavior_consumer_group',
  'nameServerAddress' = '127.0.0.1:9876'
);

CREATE TABLE rocketmq_sink (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING
) WITH (
  'connector' = 'rocketmq',
  'topic' = 'user_behavior',
  'produceGroup' = 'behavior_produce_group',
  'nameServerAddress' = '127.0.0.1:9876'
);
```

### Available Metadata

The following connector metadata can be accessed as metadata columns in a table definition.

The `R/W` column defines whether a metadata field is readable (`R`) and/or writable (`W`).
Read-only columns must be declared `VIRTUAL` to exclude them during an `INSERT INTO` operation.

| KEY            | DATA TYPE              | DESCRIPTION                                     | DEFAULT       |
| --------------   |:-----------------------------:|:---------------------------------------------------:|:--------------------:|
| topic            | STRING NOT NULL | Topic name of the RocketMQ record. | R                        |

The extended `CREATE TABLE` example demonstrates the syntax for exposing these metadata fields:

```sql
CREATE TABLE rocketmq_source (
  `topic` STRING METADATA VIRTUAL,
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING
) WITH (
  'connector' = 'rocketmq',
  'topic' = 'user_behavior',
  'consumerGroup' = 'behavior_consumer_group',
  'nameServerAddress' = '127.0.0.1:9876'
);
```

## License

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
