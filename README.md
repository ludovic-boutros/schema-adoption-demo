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

Run the same steps as branch 01 (build, run the consumer, run the producer)
against the same topic, and the consumer throws instead of printing new
orders:

```
org.apache.kafka.common.errors.SerializationException: Error deserializing JSON message from topic orders-demo
Caused by: com.fasterxml.jackson.databind.exc.InvalidFormatException:
  Cannot coerce Float value (19.99) to `java.lang.String` value
```

`OrderConsumerTest.deserialize_breaksOnTheProducersNewNumericAmountFormat`
reproduces this without needing a live cluster: it serializes an order with
the producer's current `OrderEvent`, feeds the resulting bytes to the
consumer's deserializer, and asserts it throws.

This is the cost of not having a schema: an internal, well-intentioned change
on one side silently breaks every consumer on the other, and nobody finds out
until records start failing to deserialize in production.

The next branches introduce Schema Registry to prevent this.

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
