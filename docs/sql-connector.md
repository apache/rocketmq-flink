# Table / SQL Connector

The remoting SQL connector is packaged as the fat-jar `flink-sql-connector-rocketmq`
(identifier `rocketmq`). The gRPC SQL connector is `flink-sql-connector-rocketmq-grpc`
(identifier `rocketmq-grpc`); both fat-jars relocate their dependencies and can be deployed on
the same classpath.

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

## Row format

The source maps a message body onto a row by splitting the body into lines and
then each line into fields. Both delimiters have to be set for anything but the
defaults, and a body that does not use them produces no row at all.

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `rocketmq.client.message.encoding` | String | `UTF-8` | Character set of the body |
| `rocketmq.client.message.line.delimiter` | String | `\n` | Splits the body into rows |
| `rocketmq.client.message.field.delimiter` | String | `\u0001` | Splits a row into columns |
| `rocketmq.client.message.length.check` | String | `NONE` | What to do when the field count does not match the column count: `NONE`, `SKIP`, `SKIP_SILENT`, `PAD`, `EXCEPTION`. Any other value is rejected |
| `rocketmq.source.column.error.debug` | Boolean | true | Log every field that fails to convert, instead of at most once a minute |

A column whose name matches a message property is filled from that property
rather than from the body. A single `VARBINARY` column receives the whole body
unchanged.

There is no `format` option: JSON or CSV bodies need the delimiters above, or a
DataStream job with its own `DeserializationSchema`.

## Notes

- `rocketmq.source.api.new.enable` (default true) selects the FLIP-27 source; setting it to
  false falls back to the legacy `SourceFunction` path, which has no FLIP-27 fault-tolerance
  integration and is not recommended.
- Tag and SQL92 filters are evaluated on the client: assign mode, which is what keeps queue
  allocation under the enumerator's control, can only carry a tag sub-expression to the broker.
  SQL92 uses the same `rocketmq-filter` implementation the broker uses, so predicate semantics
  are the same; only the network traffic differs.
- A missing required option is reported by key name alone. Flink's own validation message
  repeats every table option, so keep credentials out of the `WITH` clause where you can.
- Message keys can be derived from columns via `rocketmq.sink.key.columns`; a dynamic tag column
  via `rocketmq.sink.tag.dynamic.enable` + `rocketmq.sink.tag.dynamic.column`.
