# Changelog

All notable changes to `com.babelqueue:babelqueue-sqs` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

### Added
- **OpenTelemetry `traceparent` transport wiring (ADR-0028, v0.2).** `SqsPublisher`
  gains `publishWithHeaders(Envelope, Map<String,String>)` — the produce-side seam the
  optional core `com.babelqueue.otel.HeaderSender` wires to: out-of-band headers (e.g. a
  W3C `traceparent`) ride on the SQS `MessageAttributes` channel **beside** the contract
  `bq-*` attributes (`SqsAttributes.projectWithHeaders`: the contract projection wins a
  key collision, merge in sorted order, bounded by the SQS 10-attribute cap), never inside
  the frozen envelope (GR-1). New `SqsHeaders.of(Message)` surfaces a delivered message's
  attributes as a `Map<String,String>`, the consume-side seam for
  `Tracing.wrapHandler(tracer, handler, Supplier)`, so a carried `traceparent` makes the
  consumer span a true child of the producer span. A header-less publish is byte-identical
  to before; `trace_id` is preserved (GR-4); `schema_version` stays **1**. No new runtime
  dependency — the header seam is a plain `Map<String,String>`; OpenTelemetry is needed
  only by callers who opt in.

### Changed
- Require `com.babelqueue:babelqueue-core 1.5.0` (the out-of-band header-carrier seam).

## [1.0.0] - 2026-06-12

### Added
- Initial release. An Amazon SQS transport on `babelqueue-core` + AWS SDK for Java v2:
  `SqsPublisher` (canonical-envelope `SendMessage` with the §3 `MessageAttributes`
  projection — `bq-job`/`bq-trace-id`/`bq-message-id`/`bq-schema-version`/
  `bq-source-lang`/`bq-created-at`; FIFO group/dedup) and `SqsConsumer` (long-poll
  receive → URN-routed `BabelHandler`s → `DeleteMessage`; SQS-native visibility-timeout
  retry; `attempts` reconciled to `ApproximateReceiveCount − 1`, never lowering a
  runtime-incremented count; `onError`/`onUnknownUrn` hooks). Java 17, JUnit 5, JaCoCo
  ≥90% line coverage (currently ~94%); unit tests run against a fake `SqsClient` (no AWS,
  no network). The envelope is unchanged (`schema_version: 1`); SQS is purely additive.

[Unreleased]: https://github.com/BabelQueue/babelqueue-java-sqs/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/BabelQueue/babelqueue-java-sqs/releases/tag/v1.0.0
