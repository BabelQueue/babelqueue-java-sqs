package com.babelqueue.sqs;

import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import java.util.Map;
import java.util.Objects;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * Sends canonical-envelope messages to one SQS queue with the §3 attribute projection.
 * Build one with {@link #create} or {@link #builder}.
 *
 * <pre>{@code
 * SqsClient sqs = SqsClient.create();
 * SqsPublisher publisher = SqsPublisher.create(sqs, queueUrl);
 * String id = publisher.publish("urn:babel:orders:created", Map.of("order_id", 1042));
 * }</pre>
 */
public final class SqsPublisher {

    private final SqsClient client;
    private final String queueUrl;
    private final boolean fifo;
    private final String messageGroupId;
    private final boolean contentDedup;

    private SqsPublisher(Builder builder) {
        this.client = builder.client;
        this.queueUrl = builder.queueUrl;
        this.fifo = builder.fifo;
        this.messageGroupId = builder.messageGroupId;
        this.contentDedup = builder.contentDedup;
    }

    /** A publisher for a Standard queue. */
    public static SqsPublisher create(SqsClient client, String queueUrl) {
        return builder(client, queueUrl).build();
    }

    public static Builder builder(SqsClient client, String queueUrl) {
        return new Builder(client, queueUrl);
    }

    /** Publish {@code (urn, data)} as a canonical envelope; returns the message id ({@code meta.id}). */
    public String publish(String urn, Map<String, Object> data) {
        return publish(urn, data, null);
    }

    /** Publish, continuing an existing {@code traceId} (or {@code null} to mint a fresh one). */
    public String publish(String urn, Map<String, Object> data, String traceId) {
        Envelope envelope = EnvelopeCodec.make(urn, data, SqsQueues.nameFromUrl(queueUrl), traceId);
        SendMessageRequest.Builder request = SendMessageRequest.builder()
            .queueUrl(queueUrl)
            .messageBody(EnvelopeCodec.encode(envelope))
            .messageAttributes(SqsAttributes.project(envelope));
        if (fifo) {
            request.messageGroupId(messageGroupId != null ? messageGroupId : SqsQueues.nameFromUrl(queueUrl));
            if (!contentDedup) {
                request.messageDeduplicationId(envelope.meta().id());
            }
        }
        client.sendMessage(request.build());
        return envelope.meta().id();
    }

    /** Fluent builder for {@link SqsPublisher}. */
    public static final class Builder {
        private final SqsClient client;
        private final String queueUrl;
        private boolean fifo;
        private String messageGroupId;
        private boolean contentDedup;

        private Builder(SqsClient client, String queueUrl) {
            this.client = Objects.requireNonNull(client, "client");
            this.queueUrl = Objects.requireNonNull(queueUrl, "queueUrl");
        }

        /** Treat the queue as FIFO (its name must end in {@code .fifo}). */
        public Builder fifo(boolean enabled) {
            this.fifo = enabled;
            return this;
        }

        /** FIFO ordering group (default: the queue name from the URL). */
        public Builder messageGroupId(String id) {
            this.messageGroupId = id;
            return this;
        }

        /** Use the queue's content-based dedup instead of {@code meta.id} as the dedup id. */
        public Builder contentDedup(boolean enabled) {
            this.contentDedup = enabled;
            return this;
        }

        public SqsPublisher build() {
            return new SqsPublisher(this);
        }
    }
}
