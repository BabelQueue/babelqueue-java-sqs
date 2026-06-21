# BabelQueue — Amazon SQS (Java)

`com.babelqueue:babelqueue-sqs` — an Amazon SQS transport for
[BabelQueue](https://babelqueue.com), built on the AWS SDK for Java v2 and the
framework-agnostic [`babelqueue-core`](https://github.com/BabelQueue/babelqueue-java).

A canonical-envelope **publisher** and a URN-routed **consumer**, so an SQS-based Java
service speaks the same wire contract (envelope shape, URN identity, trace propagation)
as the PHP/Laravel, Python, Go, Node and .NET SDKs. Implements
[§3 of the broker-bindings contract](https://babelqueue.com).

## Install (Maven)

```xml
<dependency>
  <groupId>com.babelqueue</groupId>
  <artifactId>babelqueue-sqs</artifactId>
  <version>1.0.0</version>
</dependency>
```

It pulls `babelqueue-core` and `software.amazon.awssdk:sqs` transitively.

## Use

```java
SqsClient sqs = SqsClient.create(); // your AWS config / credentials chain
String url = "https://sqs.eu-central-1.amazonaws.com/123456789012/orders";

// produce
String id = SqsPublisher.create(sqs, url)
    .publish("urn:babel:orders:created", Map.of("order_id", 1042));

// consume
SqsConsumer consumer = SqsConsumer.builder(sqs, url)
    .handler("urn:babel:orders:created", (env, message) -> {
        // env.data(), env.traceId(), env.attempts() ...
    })
    .onError((err, env, message) -> log.warn("bad message", err))
    .build();
consumer.run(); // long-polls until the thread is interrupted
```

FIFO: `SqsPublisher.builder(sqs, url).fifo(true).build()` (the URL must end in `.fifo`).
For LocalStack/ElasticMQ, point the `SqsClient`'s endpoint there.

## Contract mapping (§3)

| Envelope | SQS |
| :--- | :--- |
| body | `MessageBody` (byte-identical across SDKs) |
| `job` (URN) | `MessageAttributes.bq-job` |
| `trace_id` | `MessageAttributes.bq-trace-id` |
| `meta.id` | `MessageAttributes.bq-message-id` |
| `meta.schema_version` | `MessageAttributes.bq-schema-version` (Number) |
| `meta.lang` | `MessageAttributes.bq-source-lang` |
| `meta.created_at` | `MessageAttributes.bq-created-at` (Number, ms) |
| `attempts` | reconciled to `ApproximateReceiveCount − 1` on receive |
| reserve / ack | visibility timeout → `DeleteMessage` |

Retry is **SQS-native**: a throwing handler leaves the message undeleted, so SQS
redelivers it after the visibility timeout (at-least-once). The poll loop never stops on
a bad message — observe via `onError` / `onUnknownUrn`. The envelope is unchanged
(`schema_version` stays `1`); SQS is purely additive.

## Trace propagation (OpenTelemetry `traceparent`, ADR-0028)

The optional core `com.babelqueue.otel` module can carry a W3C `traceparent` so a
consumer span becomes a true child of the producer span — propagated **out of band** on
the SQS `MessageAttributes` channel, beside the contract `bq-*` attributes (a contract
attribute always wins a key collision; bounded by the SQS 10-attribute cap), never inside
the frozen envelope (GR-1).

```java
// produce: HeaderSender -> SqsPublisher.publishWithHeaders
SqsPublisher publisher = SqsPublisher.create(sqs, url);
Tracing.publish(tracer, "urn:babel:orders:created", Map.of("order_id", 1042), "orders",
    (envelope, headers) -> publisher.publishWithHeaders(envelope, headers));

// consume: surface the delivered attributes for wrapHandler's Supplier
SqsConsumer.builder(sqs, url)
    .handler("urn:babel:orders:created", (env, message) ->
        Tracing.wrapHandler(tracer, h, () -> SqsHeaders.of(message)).handle(env))
    .build();
```

A header-less `publish(...)` is byte-identical to before; with no `traceparent` the
consumer falls back to the v0.1 `trace_id`-derived parent. Requires `babelqueue-core`
≥ 1.5.0. No OpenTelemetry dependency is needed unless you opt in — the seam is a plain
`Map<String,String>`.

## Build & test

```bash
mvn verify
```

Unit tests run against a fake `SqsClient` (no AWS, no network). `AWS SDK v2`'s
`SqsClient` is an interface, so the fake overrides only the three operations used.

## License

MIT
