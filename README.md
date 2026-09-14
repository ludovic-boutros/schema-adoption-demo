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

1. Create a Confluent Cloud cluster and a topic named `orders-demo` (or set
   `TOPIC` to whatever you name it).
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
