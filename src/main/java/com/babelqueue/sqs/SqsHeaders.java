package com.babelqueue.sqs;

import java.util.Map;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

/**
 * Surfaces a delivered SQS message's {@code MessageAttributes} as a flat
 * {@code Map<String, String>} — the consume-side counterpart of
 * {@link SqsPublisher#publishWithHeaders}.
 *
 * <p>It is the seam a consumer wires to the optional
 * {@code com.babelqueue.otel.Tracing#wrapHandler(io.opentelemetry.api.trace.Tracer,
 * com.babelqueue.idempotency.Handler, java.util.function.Supplier)} headers
 * {@code Supplier}, so a carried W3C {@code traceparent} (ADR-0028) makes the consumer
 * span a true child of the producer span:
 *
 * <pre>{@code
 * SqsConsumer.builder(sqs, url)
 *     .handler(urn, (env, message) -> Tracing
 *         .wrapHandler(tracer, h, () -> SqsHeaders.of(message))
 *         .handle(env))
 *     .build();
 * }</pre>
 *
 * <p>The consumer already requests {@code messageAttributeNames("All")}, so every
 * attribute — both the contract {@code bq-*} projection and any out-of-band header — is
 * delivered. Reading the headers requires no OpenTelemetry dependency; the map is a plain
 * {@code Map<String, String>}.
 */
public final class SqsHeaders {

    private SqsHeaders() {}

    /**
     * Returns the message's {@code MessageAttributes} as a {@code Map<String, String>}
     * (reading each attribute's {@code stringValue}; blank values dropped). An empty map
     * when the message carries no attributes.
     */
    public static Map<String, String> of(Message message) {
        if (message == null) {
            return Map.of();
        }
        Map<String, MessageAttributeValue> attrs = message.messageAttributes();
        return SqsAttributes.extract(attrs);
    }
}
