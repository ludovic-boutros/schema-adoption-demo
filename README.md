# 01 — No Schema, Working

The baseline: a producer and a consumer exchange `OrderEvent` records as plain
JSON. Neither one uses a schema or talks to Schema Registry — they use
Confluent's `KafkaJsonSerializer` / `KafkaJsonDeserializer`, which just
serialize a POJO to/from JSON bytes.

Producer and consumer are independent services, each with its **own copy**
of `OrderEvent`:

- `com.example.kafka.producer.OrderEvent`
- `com.example.kafka.consumer.OrderEvent`

Both currently agree: `amount` is a number (`Double`). As long as they agree,
everything works — but nothing is enforcing that agreement. That's the point
of this branch, and what breaks in the next one.

## Setup

1. Create a Confluent Cloud cluster and API key. Grant the service account
   behind that key the **ResourceOwner** role on the `orders-demo` topic (or
   whatever you set `TOPIC` to) — you don't need to create the topic
   yourself, `KafkaConfig.ensureTopicsExist(...)` creates it automatically
   via `AdminClient` on first run, but the service account needs permission
   to do so.
2. Copy `.properties.example` to `.properties` and fill in your cluster's
   bootstrap server and API key/secret.
3. Build:
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

You should see the consumer print each `OrderEvent` it receives, with
`amount` as a plain number.

Logging is configured via `src/main/resources/log4j2.xml` (Log4j2, bound
through the SLF4J API). Kafka client and Confluent serializer internals log
at `INFO`, same as the demo's own classes; bump either `org.apache.kafka`,
`io.confluent`, or `com.example.kafka` to `DEBUG` there if you want more
detail.
