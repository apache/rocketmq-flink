# Table / SQL Connector

The remoting SQL connector is packaged as the fat-jar `flink-sql-connector-rocketmq`
(identifier `rocketmq`).

## Creating tables

Required options for a source table: `rocketmq.client.endpoints`, `rocketmq.source.topic`,
`rocketmq.source.group`.

```sql
CREATE TABLE rocketmq_source (
  `id` BIGINT,
  `name` STRING,
  `message` STRING
) WITH (
  'connector' = 'rocketmq',
  'rocketmq.client.endpoints' = '127.0.0.1:9876',
  'rocketmq.source.topic' = 'flink-source',
  'rocketmq.source.group' = 'GID-flink'
);

CREATE TABLE rocketmq_sink (
  `id` BIGINT,
  `name` STRING,
  `message` STRING
) WITH (
  'connector' = 'rocketmq',
  'rocketmq.client.endpoints' = '127.0.0.1:9876',
  'rocketmq.sink.topic' = 'flink-sink',
  'rocketmq.sink.group' = 'PID-flink'
);
```

For ACL-enabled instances add `'rocketmq.client.accessKey'` / `'rocketmq.client.secretKey'`
(and `'rocketmq.client.namespace'` where applicable).

All `rocketmq.client.*`, `rocketmq.source.*` and `rocketmq.sink.*` options listed in
[remoting-connector.md](remoting-connector.md) can be used in the `WITH` clause.

## Available metadata

Read-only columns must be declared `VIRTUAL`.

| Key | Data type | R/W | Description |
| --- | --- | --- | --- |
| `topic` | STRING NOT NULL | R | Topic name of the RocketMQ record |

```sql
CREATE TABLE rocketmq_source (
  `topic` STRING METADATA VIRTUAL,
  `id` BIGINT
) WITH (
  'connector' = 'rocketmq',
  'rocketmq.client.endpoints' = '127.0.0.1:9876',
  'rocketmq.source.topic' = 'flink-source',
  'rocketmq.source.group' = 'GID-flink'
);
```

## Notes

- `rocketmq.source.api.new.enable` (default true) selects the FLIP-27 source; setting it to
  false falls back to the legacy `SourceFunction` path, which has no FLIP-27 fault-tolerance
  integration and is not recommended.
- In assign mode, tag / SQL92 filters are evaluated on the client side.
- Message keys can be derived from columns via `rocketmq.sink.key.columns`; a dynamic tag column
  via `rocketmq.sink.tag.dynamic.enable` + `rocketmq.sink.tag.dynamic.column`.
