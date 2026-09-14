# 04 — Blocked Breaking Change

A developer makes the exact same mistake as branch 02: they decide `amount`
should be a proper numeric type again, and edit the schema and the
producer's `OrderEvent` in place:

```diff
   "amount": { "type": "string", ... },
+  "amount": { "type": "number", ... },
```
```diff
-  private String amount;
+  private Double amount;
```

This time, though, that change is governed by Schema Registry.

## What happens

`OrderProducer` calls `SchemaRegistration.registerSchema(...)` at startup,
same as branch 03 — including setting the subject to `FORWARD` first (see
branch 03's "Why FORWARD, not BACKWARD"). This time the registry's
compatibility check rejects the new schema version anyway: a type change
breaks reads in *either* direction, so `FORWARD` catches it just as
`BACKWARD` would have. Compatibility mode changes who a schema evolution is
allowed to break for — it doesn't create a loophole for breaking changes
in general.

```
Schema Registry REJECTED this schema change for subject 'orders-demo-value':
Schema being registered is incompatible with an earlier schema
This is Schema Registry protecting every existing consumer from an
incompatible change (amount: string -> number). No message was sent.
```

`OrderProducer.main()` exits after logging this — **not one message is
produced**. Compare that to branch 02, where the equivalent change reached
the topic just fine and only failed downstream, inside the consumer.

`OrderProducerRejectionTest` reproduces the rejection with a mocked Schema
Registry client, no live cluster required:
- `registerOrReject_returnsEmptyWhenSchemaRegistryRejectsTheChange`
- `registerOrReject_returnsIdWhenSchemaRegistryAccepts`

The consumer is, again, byte-for-byte unchanged from branch 03 — there was
never anything for it to adapt to, because the bad schema never made it past
registration.

## Check compatibility without running the producer

The `kafka-schema-registry-maven-plugin` is now wired up in `pom.xml`, so
you can ask Schema Registry whether `order-event.schema.json` is compatible
without writing any Java or sending any message:

```
mvn initialize io.confluent:kafka-schema-registry-maven-plugin:set-compatibility
mvn initialize io.confluent:kafka-schema-registry-maven-plugin:test-compatibility
```

The first command sets `orders-demo-value` to `FORWARD` - the same mode
`OrderProducer` sets at startup (see branch 03) - so a standalone check
here matches what the producer would actually see. The second calls the
same subject, but only runs Schema Registry's compatibility check
(`testCompatibility`); nothing is registered either way. It fails the same
way `OrderProducer` fails at runtime, just without needing a JVM full of
Kafka client code to find out.

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

## Setup & Run

Same `.properties` and permissions as branch 03 (ResourceOwner on
`orders-demo` and `orders-demo-dlq`, plus Schema Registry access — topics are
still created automatically via `AdminClient` if missing).

```
mvn package
mvn exec:java -Dexec.mainClass=com.example.kafka.producer.OrderProducer
```

You should see the rejection logged and the process exit without sending
anything. The consumer (if left running from a previous branch) sees
nothing new and keeps working, undisturbed.

## The full story so far

| Branch | No schema | With Schema Registry |
|---|---|---|
| Working baseline | 01 | 03 |
| Producer changes a field's type | **02: bad data quarantined to a DLQ, requires a custom exception handler and manual triage to even survive** | **04: rejected before it ever reaches Kafka — no DLQ, no triage** |

Schema Registry didn't just catch this once — it makes catching it the
default outcome for *every* future attempt at the same mistake, instead of
leaving that distinction to be discovered — and cleaned up after the fact,
with a dead-letter topic and a human — by a consumer in production (02).

The next branch shows the other side of evolution: adding a new field is
*safe* under `FORWARD` — though, as it turns out, not for a reason quite as
simple as "additive changes always pass."

## Reset

Same as branch 03 — this branch never actually registers a new schema
version (Schema Registry rejects it), so there's nothing extra to clean up:
```
mvn exec:java -Dexec.mainClass=com.example.kafka.common.ResetDemoEnvironment
```
