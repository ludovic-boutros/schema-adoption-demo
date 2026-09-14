# Schema Adoption Demo

A walkthrough of why Confluent Schema Registry (with JSON Schema) protects
Kafka producers and consumers from breaking changes — and why plain JSON
without a schema doesn't.

The story is told across five branches, each building on the previous one.
Check out each branch in order, read its README, and run the producer/consumer
to see what happens.

| # | Branch | What it demonstrates |
|---|--------|----------------------|
| 1 | [`01-no-schema-working`](../../tree/01-no-schema-working) | Plain JSON producer & consumer (no schema). Everything works. |
| 2 | [`02-no-schema-breaking-change`](../../tree/02-no-schema-breaking-change) | The producer changes a field's type. The consumer (unchanged) can't deserialize it — a poison pill quarantined to a dead-letter topic instead of crashing the consumer outright. |
| 3 | [`03-json-schema-registry`](../../tree/03-json-schema-registry) | The producer adopts a JSON Schema in Schema Registry, with the schema ID in a Kafka header. The consumer is untouched and keeps working. |
| 4 | [`04-json-schema-safe-evolution`](../../tree/04-json-schema-safe-evolution) | The producer safely evolves the schema by adding a new field. Schema Registry allows it; the consumer is still untouched and keeps working. |
| 5 | [`05-json-schema-blocked-breaking-change`](../../tree/05-json-schema-blocked-breaking-change) | The producer attempts the same kind of breaking change as step 2. Schema Registry rejects it before any bad data reaches the topic. |

## Prerequisites

- Java 17+, Maven 3.9+
- A Confluent Cloud cluster and environment (Schema Registry is provisioned
  per-environment and is needed from branch 03 onward)
- A Confluent Cloud API key (cluster) and, from branch 03 onward, a Schema
  Registry API key
- The service account behind those API keys needs the **ResourceOwner**
  role on the `orders-demo` and `orders-demo-dlq` topics. Every branch
  creates both topics automatically via `AdminClient` if they don't already
  exist (`KafkaConfig.ensureTopicsExist`) — you never need to create a topic
  by hand, but the service account does need permission to create them.

Every branch reads connection details from a local `.properties` file
(gitignored — never committed). Copy `.properties.example` to `.properties`
and fill in your own values before running anything.

## The scenario

An `OrderEvent` (`orderId`, `customerId`, `amount`, `status`) is produced to
the `orders-demo` topic and read back by an independent consumer. Producer
and consumer are modeled as separate services, each maintaining its own copy
of the data model — exactly as they would be in a real system, which is why
the two can drift apart in the first place.

```
git checkout 01-no-schema-working
cat README.md
```

Then move on to each subsequent branch in order.
