# 02 — No Schema, Breaking Change

A developer working on the producer decides `amount` should be a proper
numeric type (`Double`) instead of the string it's always been — easier to
sum, compare, and do math on. Nothing stops them — there's no schema, so
nothing checks whether the consumer can still read the new shape.

```diff
- private String amount;
+ private Double amount;
```

Only `com.example.kafka.producer.OrderEvent` changed. The consumer's own
copy, `com.example.kafka.consumer.OrderEvent`, still declares `amount` as a
`String` — untouched, unaware anything changed.

## What happens

`KafkaJsonDeserializer` fails to turn a JSON number into a `String`. Left
alone, that would be a classic **poison pill**: `KafkaConsumer.poll()` throws
`RecordDeserializationException` for that record, offsets are never
committed past it (`enable.auto.commit=false`, and we only commit after a
batch is successfully processed), so retrying — or restarting the process —
just fetches the same broken record and throws again. Forever.

This branch's consumer doesn't crash. It catches
`RecordDeserializationException` (KIP-334 — it carries the exact
`TopicPartition`/offset and the raw, untouched key/value bytes of the record
that failed) around `poll()`, and:

1. Publishes the raw bytes to a dead-letter topic, `orders-demo-dlq`, for
   later inspection.
2. Calls `consumer.seek(topicPartition, offset + 1)` to skip past the bad
   record.
3. Keeps polling — the rest of the topic, and any new orders, keep flowing.

```
Poison pill on orders-demo-0 at offset 4: Cannot coerce Float value (19.99)
  to `java.lang.String` value
Published poison pill to DLQ topic 'orders-demo-dlq' at offset 0
Skipped past offset 4 on orders-demo-0 - the consumer keeps running instead
  of crashing.
```

`OrderConsumerTest.deserialize_breaksOnTheProducersNewNumericAmountFormat`
reproduces the underlying deserialization failure without needing a live
cluster, and `OrderConsumerPoisonPillTest.handlePoisonPill_publishesRawBytesToDlqAndSeeksPastTheOffset`
reproduces the recovery path — a `MockProducer` captures what's published to
the DLQ, and a mocked `Consumer` verifies `seek()` is called with the right
offset.

This is *not* a fix. `amount` was never delivered to anything that cares
about it — it's quarantined in `orders-demo-dlq` waiting for a human to look
at it and decide what to do. No schema meant nobody found out about the
incompatible change until it happened in production, and surviving it still
took a dead-letter topic, a custom exception handler, and manual triage.

The next branches introduce Schema Registry so this class of change is
rejected before it ever reaches the topic — no DLQ, no triage, no lost data.

## Setup

**JDK 21** — `pom.xml` targets `maven.compiler.release=21`. Point
`JAVA_HOME` at a JDK 21 install on the command line (the same one your
IDE's project SDK uses) so `mvn` and your IDE compile and run identically.

Same cluster and `.properties` as branch 01, plus one addition: grant the
service account behind your API key the **ResourceOwner** role on
`orders-demo-dlq` too (in addition to `orders-demo`), since this branch
introduces that topic and creates it automatically the same way.

```
mvn package
```

## Run

Consumer (leave running in one terminal):
```
mvn exec:java -Dexec.mainClass=com.example.kafka.consumer.OrderConsumer
```

Producer (in another terminal):
```
mvn exec:java -Dexec.mainClass=com.example.kafka.producer.OrderProducer
```

Watch the consumer's terminal: the poison pill is logged and shipped to
`orders-demo-dlq`, and the consumer keeps running.

## Reset

```
mvn exec:java -Dexec.mainClass=com.example.kafka.common.ResetDemoEnvironment
```
Now also deletes and recreates `orders-demo-dlq` alongside `orders-demo`,
clearing out any poison pills quarantined by a previous run.
