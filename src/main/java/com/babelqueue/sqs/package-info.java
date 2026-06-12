/**
 * Amazon SQS transport for BabelQueue — a canonical-envelope {@link com.babelqueue.sqs.SqsPublisher}
 * and a URN-routed {@link com.babelqueue.sqs.SqsConsumer} over the AWS SDK for Java v2,
 * on the framework-agnostic {@code babelqueue-core}.
 *
 * <p>It implements §3 of the broker-bindings contract: the canonical envelope is the
 * message body, projected onto native SQS {@code MessageAttributes}
 * ({@code bq-job}/{@code bq-trace-id}/{@code bq-message-id}/{@code bq-schema-version}/
 * {@code bq-source-lang}/{@code bq-created-at}). Consuming uses the visibility-timeout
 * reservation model (receive → process → delete); the authoritative attempt count is the
 * broker's {@code ApproximateReceiveCount}, surfaced as {@code attempts = count - 1}. The
 * envelope is unchanged ({@code schema_version} stays 1); SQS is purely additive.
 *
 * <p>Full spec: <a href="https://babelqueue.com">babelqueue.com</a>.
 */
package com.babelqueue.sqs;
