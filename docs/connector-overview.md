# Flink MQ Connectors Overview

> 基于 `flink-connectors` 目录的分析结果，涵盖 Source/Sink 支持、消费模型、Spotless 代码格式化等维度。

## 连接器全景对比

| 厂商 | 产品 | Java | 分区指派模型 | Pop/共享消费模型 | Source | Sink | 新 Source API (FLIP-27) | 新 Sink API (FLIP-143) | Table/SQL API | SQL Connector | Spotless |
| --- | --- | --- | :--- | :--- | :---: | :---: | :--- | :--- | :--- | :---: | :---: |
| Apache | **Kafka** | 11/17 ¹ | ✅ | ❌ | ✅ | ✅ | ✅ `KafkaSource` | ✅ `KafkaSink` | ✅ | ✅ | ✅ |
| Apache | **RocketMQ** | 11 ² | ✅ Clustering | ✅ Pop ³ | ✅ | ✅ | ✅ `RocketMQSource` | ✅ `RocketMQSink` | ✅ | ✅ | ✅ |
| Apache | **Pulsar** | 8 | ✅ Exclusive | ✅ Shared ⁴ | ✅ | ✅ | ✅ `PulsarSource` | ✅ `PulsarSink` | ✅ | ✅ | ✅ |
| Rabbit Technologies | **RabbitMQ** | 8 | ❌ | ✅ | ✅ | ✅ | ❌ `RMQSource` ⁵ | ❌ `RMQSink` ⁵ | ❌ | ✅ | ✅ |
| Google | **Cloud Pub/Sub** | 8 | ❌ | ✅ | ✅ | ✅ | ❌ `PubSubSource` ⁵ | ✅ `PubSubSinkV2` | ✅ | ❌ | ✅ |
| AWS | **Kinesis Data Streams** | 8 | ✅ Shard | ❌ | ✅ | ✅ | ✅ `KinesisStreamsSource` | ✅ AsyncSinkBase | ✅ | ✅ | ✅ |
| AWS | **Kinesis Firehose** | 8 | — | — | ❌ | ✅ | — | ✅ AsyncSinkBase | ✅ | ✅ | ✅ |
| AWS | **SQS** | 8 | — | ✅ | ❌ | ✅ | — | ✅ AsyncSinkBase | ✅ | ❌ | ✅ |
| AWS | **DynamoDB Streams** | 8 | ✅ Shard | ❌ | ✅ | ✅ | ✅ `DynamoDbStreamsSource` | ✅ AsyncSinkBase | ✅ | ✅ | ✅ |

## 列说明

### Table/SQL API vs SQL Connector

| 列 | 含义 | 本质 |
| --- | --- | --- |
| **Table/SQL API** | 连接器代码中实现了 Flink Table/SQL 接口（`DynamicTableSource`、`DynamicTableSink` 等） | **代码层面** — 有没有写 SQL 集成的 Java 类 |
| **SQL Connector** | 存在 `flink-sql-connector-*` shade 模块，打成一个包含所有依赖的 fat JAR | **分发层面** — 能不能直接丢进 `lib/` 目录就用 |

典型差异场景：GCP Pub/Sub

- Table/SQL API ✅ — 代码里有 `PubSubDynamicSink`、`PubSubDynamicSinkFactory`，支持 SQL DDL
- SQL Connector ❌ — 没有打包成 shade JAR

这意味着用户虽然可以在 SQL 中声明 `'connector' = 'gcp-pubsub'`，但需要自己手动管理依赖（把 connector JAR + gRPC JAR + Google SDK JAR 等全部放进 classpath），而不能像 Kafka 那样直接下载一个 `flink-sql-connector-kafka-xxx.jar` 丢进 `lib/` 就完事。

总结：Table/SQL API 是"能不能用"，SQL Connector 是"好不好用"。

## 脚注

1. **Kafka Java 版本**: 继承 `flink-connector-parent` 2.0.0，source 11 / target 17（字节码）。
2. **RocketMQ Java 版本**: 显式覆盖 `maven-compiler-plugin` 为 source 11 / target 11。
3. **RocketMQ Pop**: RocketMQ 产品本身支持 Pop 消费模式，但 Flink 连接器未实现。连接器使用 `DefaultLitePullConsumer` + Flink 侧分区指派（Clustering），硬编码 `MessageModel.CLUSTERING`。
4. **Pulsar Shared**: Flink 连接器实际硬编码为 `Exclusive`（分区模型）。Table API 暴露了 `Shared` 选项，但存在 bug 未正确传递给底层 Source，实际不可用。
5. **旧 API**: 仍使用 `RichSourceFunction` / `RichSinkFunction`，未迁移到 FLIP-27 / FLIP-143 新 API。

## 消费模型说明

**分区指派模型** — 消息按 partition/shard/queue 有序，消费者能看到自己被分配了哪些分区，适合需要严格顺序或精确位点管理的场景。

**Pop / 共享消费模型** — 消费者不感知分区，broker 将消息分发给组内任意空闲消费者，适合高吞吐、不关心顺序的负载均衡场景。

## Spotless 代码格式化

所有连接器均使用 Spotless 进行代码格式化：

- 继承自 `flink-connector-parent` 统一定义规则
- 每个连接器在 `<build><plugins>` 中显式声明 `com.diffplug.spotless:spotless-maven-plugin`
- 无任何连接器设置 `<spotless.skip>true</spotless.skip>`
- Flink 主仓库 Spotless 版本：`2.43.0`
