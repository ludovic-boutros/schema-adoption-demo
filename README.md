# 05 — Safe Evolution (Forward Compatibility)

Branch 04's retype attempt is abandoned - `amount` goes back to being a
`String`. Instead, a new requirement comes in: a billing analytics service
wants to do math on the total without parsing a string itself. The producer
adds a new, optional field alongside the existing one:

```diff
   "amount": { "type": "string", ... },
+  "amountNumeric": { "type": "number", ... },
```
```diff
   private String amount;
+  private Double amountNumeric;
```

## Why this one actually needs FORWARD

Branch 03 set `orders-demo-value` to `FORWARD` because the producer owns
this data. It would be easy to assume an additive, optional field like
`amountNumeric` passes *any* compatibility mode - nothing existing was
removed or retyped, after all. It doesn't: `order-event.schema.json` has
`"additionalProperties": true` (an open content model), and Schema
Registry's JSON Schema checker rejects an optional property added to an
open-content-model schema under `BACKWARD`
(`OPTIONAL_PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL`), even though nothing
existing was touched. Under `FORWARD` it passes. So this branch isn't just
a demonstration of safe evolution - it's a demonstration of why branch 03
picked `FORWARD` in the first place: the producer-owned schema keeps
evolving additively, and that only works because the compatibility mode
matches the evolution.

## What happens

Registration succeeds and returns a new schema ID. The producer sends
messages with both `amount` and `amountNumeric` populated. The consumer,
running `OrderConsumer.buildValueDeserializer()` from branch 03 completely
unmodified, ignores `amountNumeric` (`FAIL_ON_UNKNOWN_PROPERTIES` is `false`)
and keeps reading `amount` exactly as before -
`OrderConsumerTest.deserialize_ignoresNewAmountNumericFieldTheConsumerDoesNotKnowAbout`
proves it.

## Check compatibility without running the producer

The `kafka-schema-registry-maven-plugin` (wired up in branch 04's `pom.xml`)
lets you ask Schema Registry whether `order-event.schema.json` is compatible
without writing any Java or sending any message:

```
mvn initialize io.confluent:kafka-schema-registry-maven-plugin:set-compatibility
mvn initialize io.confluent:kafka-schema-registry-maven-plugin:test-compatibility
```

Unlike branch 04, this succeeds: the same additive, optional
`amountNumeric` field that `OrderProducer` registers successfully at
runtime also passes Schema Registry's compatibility check on its own,
before you write a single message. The first command sets
`orders-demo-value` to `FORWARD` (redundant here since branch 03 already
set it, but this keeps the standalone check reproducible even against a
freshly reset subject); the second only runs `testCompatibility` - nothing
is registered either way.

It authenticates using `SCHEMA_REGISTRY_URL`, `SR_API_KEY`, and
`SR_API_SECRET` from your local `.properties` file - a `properties-maven-plugin`
execution loads that file into Maven properties (`initialize` phase, silently
skipped if `.properties` doesn't exist yet), so none of those values are ever
written into `pom.xml`.

**The `initialize` phase must be listed first.** A direct `mvn plugin:goal`
invocation runs *only* that one goal - it skips every phase-bound execution,
including the `properties-maven-plugin` execution above, so
`SCHEMA_REGISTRY_URL` and friends would silently stay unresolved and the
plugin would fail with a `NullPointerException` (`baseUrl` is null) instead
of an authentication or compatibility error. Prefixing the command with
`initialize` runs the lifecycle up through that phase first (loading
`.properties` into Maven properties), then the goal, in the same Maven
invocation.

Try skipping `set-compatibility` against a freshly reset subject (or run
`ResetDemoEnvironment` first) and re-running `test-compatibility` - it
fails with `OPTIONAL_PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL` against
Confluent Cloud's `BACKWARD` default, reproducing exactly the wrinkle
described above.

## Setup & Run

Same `.properties` and permissions as branches 03/04 (ResourceOwner on
`orders-demo` and `orders-demo-dlq`, plus Schema Registry access — topics are
still created automatically via `AdminClient` if missing).

```
mvn package
mvn exec:java -Dexec.mainClass=com.example.kafka.consumer.OrderConsumer
mvn exec:java -Dexec.mainClass=com.example.kafka.producer.OrderProducer
```

You should see the producer log the new schema ID and the `FORWARD`
compatibility update, and the consumer keep printing orders using only the
fields it has always known about.

## The full story

| Branch | No schema | With Schema Registry |
|---|---|---|
| Working baseline | 01 | 03 |
| Producer changes a field's type | **02: bad data quarantined to a DLQ, requires a custom exception handler and manual triage to even survive** | **04: rejected before it ever reaches Kafka — no DLQ, no triage** |
| Adding a new field | (no protection either way) | **05: accepted under FORWARD compatibility, consumer unaffected** |

Schema Registry didn't make evolution impossible - it made the *safe* kind
of evolution (05) possible and the *unsafe* kind (04) impossible, once you
pick the compatibility mode that actually fits how the schema is owned and
shaped. `FORWARD` was the right call here: the producer owns this data
(branch 03), and this schema's open content model means an additive field
needs `FORWARD` specifically to pass - not "any mode," and not
automatically. Either way, the distinction is caught at registration time,
instead of being discovered - and cleaned up after the fact, with a
dead-letter topic and a human - by a consumer in production (02).

## Reset

```
mvn exec:java -Dexec.mainClass=com.example.kafka.common.ResetDemoEnvironment
```
Deletes the `orders-demo-value` subject (soft delete followed by a
permanent one, so a fresh registration after reset isn't rejected as
"incompatible" with a schema that's supposedly gone) and recreates
`orders-demo` and `orders-demo-dlq` empty.
