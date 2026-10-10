# Changelog

All notable changes to `com.babelqueue:babelqueue-sqs` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

### Fixed

- `SqsConsumer` now applies the §3.7 schema-version gate **before** decoding the body: when the
  `bq-schema-version` message attribute is present and is not the supported version (`1`), the body is
  never decoded and the handler is never called; the message is reported via `onError` (with a `null`
  envelope) and is **not** deleted, so SQS redrives it to the DLQ. A missing or blank attribute (empty or
  ASCII-whitespace-only) keeps the existing decode path. Cross-SDK parity (GR-5): the value is compared
  exactly, with no trimming.
- `onError` may now receive a `null` envelope for a schema-version gate rejection (the body is not decoded, so
  there is no envelope); callbacks that dereference the envelope should null-check it.

## [1.2.0] - 2026-10-03

MINOR release: the failure path's behaviour and the public `Builder` API change.

### Changed
- **Release now follows broker-bindings.md §3 — behaviour change.** A throwing handler's
  message is always released with `ChangeMessageVisibility(ReceiptHandle, VisibilityTimeout =
  backoff)` instead of being left to wait out the queue's full visibility timeout; it is still
  never deleted. The backoff is the new `Builder.releaseDelaySeconds(int)`, **default `0` =
  redeliver now**. Previously a failing message reappeared only after the visibility timeout
  (e.g. 30 s). Set `releaseDelaySeconds(N)` to keep a delay between attempts.
- **Poison-loop risk.** With the default `0`, a message that fails permanently is redelivered on
  every receive, and a short outage can burn through `maxReceiveCount` in under a second.
  **Configure a `RedrivePolicy` on the queue** (`deadLetterTargetArn` + `maxReceiveCount`) so SQS
  moves such a message to a native DLQ, and pick a non-zero `releaseDelaySeconds` if the
  handler depends on something that can be briefly unavailable.
- Out-of-range delays are **clamped** to 0–43200 s (the SQS 12 h cap), like the Go and Python
  transports; they no longer throw.
- Require `com.babelqueue:babelqueue-core 1.8.0` (unknown-key preservation, `Envelope.withAttempts`).

### Fixed
- A failed `DeleteMessage` after a **successful** handler (throttling, network, stale receipt
  handle) is no longer treated as a handler failure: it is reported to `onError` as the new
  `SqsDeleteException` (broker error as its cause) and the message is **not** released, so it
  is redelivered only after its visibility timeout instead of immediately. The same applies to
  the `delete`/`dead_letter` unknown-URN strategies; a failed delete never escapes `poll()`.
- A failed release (`ChangeMessageVisibility` rejected: stale receipt handle, throttling,
  network) no longer escapes `poll()`/`run()`. The handler error is reported to `onError`
  first, then the release error; the batch continues and SQS still redelivers the message
  once its visibility timeout expires.
- `attempts` reconciliation now preserves unknown top-level / `meta` keys (core 1.8.0 extras),
  so a newer producer's additions reach the handler unchanged.

### Added
- **Unknown-URN strategy** (`Builder.unknownUrnStrategy(String)`, `fail` | `delete` |
  `release` | `dead_letter`, the core `UnknownUrnStrategy` names). `release` calls
  `ChangeMessageVisibility` with `Builder.unknownUrnReleaseDelaySeconds(int)` (**default `0`**,
  matching the Python and Node transports; clamped to 0–43200); `dead_letter` degrades to
  `delete` (this transport has no DLQ publisher — the contract's "DLQ disabled" rule). Default
  stays backward compatible: `fail`, or `delete` when only `onUnknownUrn` is set.
  `onUnknownUrn` is now invoked before every strategy.
- `SqsConsumer.MAX_VISIBILITY_TIMEOUT_SECONDS` (43200).
- `.github/dependabot.yml` (Maven + GitHub Actions, weekly).

## [1.1.0] - 2026-06-21

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
