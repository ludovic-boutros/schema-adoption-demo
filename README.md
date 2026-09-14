# 03 — Add JSON Schema Registry (Producer Side Only)

Branch 02 ended with an incident: the producer silently retyped `amount`
from `String` to `Double` and broke the consumer. This branch does two
things:

1. **Fixes the incident** — `amount` goes back to being a `String`, its
   original working type.
2. **Adopts Schema Registry** so the next time someone tries a change like
   that, it's caught before a single message reaches Kafka, instead of
   being discovered downstream in production.

It touches only the producer: `pom.xml`, `SchemaRegistration.java`,
`HeaderEncodedJsonSerializer.java`, `order-event.schema.json`, and
`OrderProducer.java`. The consumer is untouched — check for yourself:
```
git show --stat HEAD -- src/main/java/com/example/kafka/consumer
```

## What changed on the producer

- `src/main/resources/schemas/order-event.schema.json` — a JSON Schema
  describing the current `OrderEvent` shape (`amount` as a string, matching
  the working baseline).
- `SchemaRegistration` registers that schema against Schema Registry
  **explicitly, at startup** (`SchemaRegistration.registerSchema(...)`) —
  never via serializer auto-registration. This is what CI/CD would do in a
  real system.
- `HeaderEncodedJsonSerializer` wraps Confluent's plain `KafkaJsonSerializer`
  and additionally writes the resulting schema ID into a `schema-id` Kafka
  record header (plus `schema-subject`), instead of the usual
  Confluent wire-format prefix. **The JSON payload bytes are unchanged.**

## Why the consumer doesn't need to change

The consumer's deserializer (`KafkaJsonDeserializer`) reads plain JSON bytes
and has no idea headers exist. Since the payload format is byte-for-byte the
same as branch 01's working baseline, the exact same consumer binary from
branch 02 (with its poison-pill/DLQ handling intact) keeps working without
modification, recompilation, or redeployment.

## Setup

1. **JDK 21** — `pom.xml` targets `maven.compiler.release=21`. Point
   `JAVA_HOME` at a JDK 21 install on the command line (the same one your
   IDE's project SDK uses) so `mvn` and your IDE compile and run identically.
2. Make sure your Confluent Cloud environment has Schema Registry
   provisioned (one Schema Registry per environment).
3. Grant the service account behind your API key the **ResourceOwner** role
   on the `orders-demo` and `orders-demo-dlq` topics (same as branches
   01/02), plus permission to manage schemas for the `orders-demo-value`
   subject (Schema Registry RBAC is separate from cluster RBAC). Topics are
   still created automatically via `AdminClient` if missing.
4. Copy `.properties.example` to `.properties`, filling in
   `SCHEMA_REGISTRY_URL`, `SR_API_KEY`, and `SR_API_SECRET` in addition to the
   cluster settings from before.
5. Build: `mvn package`

## Run

Consumer (unchanged from branch 02):
```
mvn exec:java -Dexec.mainClass=com.example.kafka.consumer.OrderConsumer
```

Producer (now schema-aware, and back to sending `amount` as a string):
```
mvn exec:java -Dexec.mainClass=com.example.kafka.producer.OrderProducer
```

You should see the producer log the schema ID it registered, and the
consumer print orders exactly as it did in branch 01 — no code changes, no
restart required beyond picking up new messages.

The next branch shows Schema Registry **blocking** the same class of
breaking change that hurt branch 02. The one after that shows a *safe*
evolution (adding a new field) sailing through.

## Reset

```
mvn exec:java -Dexec.mainClass=com.example.kafka.common.ResetDemoEnvironment
```
Also deletes the `orders-demo-value` subject from Schema Registry (soft
delete followed by a permanent delete), in addition to recreating
`orders-demo` and `orders-demo-dlq`. The permanent delete matters: a
soft-deleted subject still counts toward compatibility checks, so without
it a fresh registration after reset could be rejected as "incompatible"
with a schema that's supposedly gone.
