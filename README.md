# Apache Flink RocketMQ Connector

[![Build](https://github.com/apache/rocketmq-flink/actions/workflows/build.yml/badge.svg)](https://github.com/apache/rocketmq-flink/actions/workflows/build.yml)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

This repository contains the Apache Flink RocketMQ connector. It provides Flink sources and sinks
that read from and write to Apache RocketMQ, for both the DataStream API and the Table/SQL API.

Two connector tracks are shipped:

| Track | Module | RocketMQ client | Best for |
| --- | --- | --- | --- |
| **Remoting** | `flink-connector-rocketmq` | `rocketmq-client` (remoting protocol) | Classic topics, FLIP-27 source + SinkV2 sink, SQL |
| **gRPC** | `flink-connector-rocketmq-grpc` | `rocketmq-client-java` (gRPC protocol) | RocketMQ 5.x lite topics, downstream ack / fair throttling |

The SQL fat-jars are packaged by `flink-sql-connector-rocketmq` and
`flink-sql-connector-rocketmq-grpc`.

## Apache Flink

Apache Flink is an open source stream processing framework with powerful stream- and
batch-processing capabilities.

Learn more about Flink at [https://flink.apache.org/](https://flink.apache.org/)

## Apache RocketMQ

Apache RocketMQ is a cloud native messaging and streaming platform, making it simple to build
event-driven applications.

Learn more about RocketMQ at [https://rocketmq.apache.org/](https://rocketmq.apache.org/)

## Building the Apache Flink RocketMQ Connector from Source

Prerequisites:

* Unix-like environment (we use Linux, Mac OS X)
* Git
* Maven (we recommend version 3.8.6)
* Java 11

```
git clone https://github.com/apache/rocketmq-flink.git
cd rocketmq-flink
mvn clean package -DskipTests
```

The resulting jars can be found in the `target` directory of the respective module.

Before opening a pull request, format the code and run the tests:

```
mvn spotless:apply clean package
```

## Quick Start

### DataStream source & sink (remoting)

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(3000);

RocketMQSource<String> source = RocketMQSource.<String>builder()
        .setEndpoints("127.0.0.1:9876")
        .setGroupId("GID-flink")
        .setTopics("flink-source")
        .setMinOffsets(OffsetsSelector.latest())
        .setBodyOnlyDeserializer(new SimpleStringSchema())
        .build();

RocketMQSink<String> sink = RocketMQSink.<String>builder()
        .setEndpoints("127.0.0.1:9876")
        .setGroupId("PID-flink")
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .setSerializer(new MySerializationSchema("flink-sink"))
        .build();

env.fromSource(source, WatermarkStrategy.noWatermarks(), "rocketmq-source")
        .sinkTo(sink);
env.execute();
```

### SQL

```sql
CREATE TABLE rocketmq_source (
  `id` BIGINT,
  `message` STRING
) WITH (
  'connector' = 'rocketmq',
  'rocketmq.client.endpoints' = '127.0.0.1:9876',
  'rocketmq.source.topic' = 'flink-source',
  'rocketmq.source.group' = 'GID-flink'
);
```

### gRPC lite consumer (RocketMQ 5.x)

```java
RocketMQGrpcSource<String> source = RocketMQGrpcSource.<String>builder()
        .setEndpoints("127.0.0.1:8081")
        .setConsumerGroup("GID-lite")
        .setMainTopic("LiteMainTopic")        // wildcard-subscribes all lite topics under it
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();
```

The gRPC source emits `AckableMessage<OUT>` and never acks by itself: the downstream operators own
the ack / throttle decision. See the gRPC connector doc for the ack operator wirings.

## Documentation

Connector documentation is located in the `docs/` directory of this repository:

| Document | Content |
| --- | --- |
| [docs/remoting-connector.md](docs/remoting-connector.md) | Remoting DataStream connector: usage and all configuration options |
| [docs/grpc-connector.md](docs/grpc-connector.md) | gRPC lite connector: prerequisites, downstream ack / throttling, options |
| [docs/sql-connector.md](docs/sql-connector.md) | Table/SQL connector: DDL, metadata columns, fat-jar notes |
| [docs/legacy-connector.md](docs/legacy-connector.md) | Legacy `RocketMQSourceFunction` / `RocketMQSink` (deprecated) |
| [docs/connector-overview.md](docs/connector-overview.md) | Feature comparison with other messaging connectors |

The documentation of Apache Flink is located on the website
[https://flink.apache.org](https://flink.apache.org), and the documentation of Apache RocketMQ on
[https://rocketmq.apache.org](https://rocketmq.apache.org).

## Developing

The Flink committers use IntelliJ IDEA to develop the Flink codebase. IntelliJ supports Maven out
of the box. Check out the
[Setting up IntelliJ](https://nightlies.apache.org/flink/flink-docs-master/flinkDev/ide_setup.html#intellij-idea)
guide for details.

Run `mvn spotless:apply` before committing to keep the code style consistent.

## Support

Don't hesitate to ask!

Contact the developers and community on the
[Apache Flink mailing lists](https://flink.apache.org/community.html#mailing-lists) or the
[Apache RocketMQ mailing lists](https://rocketmq.apache.org/contact/) if you need any help.

[Open an issue](https://github.com/apache/rocketmq-flink/issues) if you find a bug in the
connector.

## Fork and Contribute

This is an active open-source project. We are always open to people who want to use the system or
contribute to it. If you find a bug or want a new feature, open an issue first and discuss it with
the community. See [how to contribute to Apache RocketMQ](https://rocketmq.apache.org/contribute/)
and [how to contribute to Apache Flink](https://flink.apache.org/contributing/how-to-contribute.html).

## About

Apache Flink and Apache RocketMQ are open source projects of The Apache Software Foundation (ASF).
