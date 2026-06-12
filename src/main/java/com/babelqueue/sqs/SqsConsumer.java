package com.babelqueue.sqs;

import com.babelqueue.BabelQueueException;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import com.babelqueue.UnknownUrnException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

/**
 * Polls an SQS queue, decodes and validates each message, routes it to the handler
 * registered for its URN, and deletes it on success. A throwing handler leaves the
 * message undeleted — SQS redelivers it after the visibility timeout (at-least-once);
 * {@code attempts} is reconciled to {@code ApproximateReceiveCount - 1} for the handler.
 * The poll loop never stops on a bad message — observe via {@code onError}/{@code onUnknownUrn}.
 */
public final class SqsConsumer {

    /** Notified of a non-conformant envelope, an unmapped URN (no {@code onUnknownUrn}), or a throwing handler. */
    @FunctionalInterface
    public interface ErrorHandler {
        void onError(Throwable error, Envelope envelope, Message message);
    }

    /** Called instead of erroring when a URN has no handler; the message is then deleted. */
    @FunctionalInterface
    public interface UnknownUrnHandler {
        void onUnknownUrn(Envelope envelope, Message message);
    }

    private final SqsClient client;
    private final String queueUrl;
    private final Map<String, BabelHandler> handlers;
    private final int waitTimeSeconds;
    private final Integer visibilityTimeout;
    private final int maxMessages;
    private final ErrorHandler onError;
    private final UnknownUrnHandler onUnknownUrn;

    private SqsConsumer(Builder builder) {
        this.client = builder.client;
        this.queueUrl = builder.queueUrl;
        this.handlers = Map.copyOf(builder.handlers);
        this.waitTimeSeconds = builder.waitTimeSeconds;
        this.visibilityTimeout = builder.visibilityTimeout;
        this.maxMessages = builder.maxMessages;
        this.onError = builder.onError;
        this.onUnknownUrn = builder.onUnknownUrn;
    }

    public static Builder builder(SqsClient client, String queueUrl) {
        return new Builder(client, queueUrl);
    }

    /** Receive one batch, route each message, delete the ones handled. Returns the batch size. */
    public int poll() {
        ReceiveMessageRequest.Builder request = ReceiveMessageRequest.builder()
            .queueUrl(queueUrl)
            .maxNumberOfMessages(maxMessages)
            .waitTimeSeconds(waitTimeSeconds)
            .messageAttributeNames("All")
            .attributeNamesWithStrings("ApproximateReceiveCount");
        if (visibilityTimeout != null) {
            request.visibilityTimeout(visibilityTimeout);
        }
        List<Message> messages = client.receiveMessage(request.build()).messages();
        for (Message message : messages) {
            handle(message);
        }
        return messages.size();
    }

    /** Poll until the current thread is interrupted (each poll long-polls, so this does not busy-loop). */
    public void run() {
        run(() -> !Thread.currentThread().isInterrupted());
    }

    /** Poll while {@code shouldContinue} returns true. */
    public void run(BooleanSupplier shouldContinue) {
        while (shouldContinue.getAsBoolean()) {
            poll();
        }
    }

    private void handle(Message message) {
        String body = message.body() == null ? "" : message.body();
        Envelope envelope = reconcile(
            EnvelopeCodec.decode(body),
            message.attributesAsStrings().get("ApproximateReceiveCount"));

        if (!EnvelopeCodec.accepts(envelope)) {
            report(new BabelQueueException("Rejected a non-conformant BabelQueue envelope from SQS."),
                envelope, message);
            return;
        }

        String urn = EnvelopeCodec.urn(envelope);
        BabelHandler handler = handlers.get(urn);
        if (handler == null) {
            if (onUnknownUrn != null) {
                onUnknownUrn.onUnknownUrn(envelope, message);
                delete(message);
            } else {
                report(new UnknownUrnException(urn), envelope, message);
            }
            return;
        }

        try {
            handler.handle(envelope, message);
            delete(message);
        } catch (Exception error) {
            // Leave the message undeleted — SQS redelivers after the visibility timeout.
            report(error, envelope, message);
        }
    }

    /**
     * Set {@code attempts} to max(current, ApproximateReceiveCount - 1): a first delivery
     * reads 0, a natively-redelivered message reflects its true count, and a
     * runtime-incremented counter is never lowered.
     */
    private static Envelope reconcile(Envelope envelope, String receiveCount) {
        if (receiveCount == null) {
            return envelope;
        }
        int count;
        try {
            count = Integer.parseInt(receiveCount.strip());
        } catch (NumberFormatException ex) {
            return envelope;
        }
        if (count <= 1) {
            return envelope;
        }
        int reconciled = count - 1;
        if (reconciled <= envelope.attempts()) {
            return envelope;
        }
        return new Envelope(
            envelope.job(), envelope.traceId(), envelope.data(), envelope.meta(),
            reconciled, envelope.deadLetter());
    }

    private void delete(Message message) {
        if (message.receiptHandle() == null) {
            return;
        }
        client.deleteMessage(DeleteMessageRequest.builder()
            .queueUrl(queueUrl)
            .receiptHandle(message.receiptHandle())
            .build());
    }

    private void report(Throwable error, Envelope envelope, Message message) {
        if (onError != null) {
            onError.onError(error, envelope, message);
        }
    }

    /** Fluent builder for {@link SqsConsumer}. */
    public static final class Builder {
        private final SqsClient client;
        private final String queueUrl;
        private final Map<String, BabelHandler> handlers = new HashMap<>();
        private int waitTimeSeconds = 20;
        private Integer visibilityTimeout;
        private int maxMessages = 10;
        private ErrorHandler onError;
        private UnknownUrnHandler onUnknownUrn;

        private Builder(SqsClient client, String queueUrl) {
            this.client = Objects.requireNonNull(client, "client");
            this.queueUrl = Objects.requireNonNull(queueUrl, "queueUrl");
        }

        /** Register {@code handler} for {@code urn} (the last registration wins). */
        public Builder handler(String urn, BabelHandler handler) {
            this.handlers.put(urn, handler);
            return this;
        }

        public Builder handlers(Map<String, BabelHandler> handlers) {
            this.handlers.putAll(handlers);
            return this;
        }

        /** Long-poll wait seconds (default 20). */
        public Builder waitTimeSeconds(int seconds) {
            this.waitTimeSeconds = seconds;
            return this;
        }

        /** Reservation window applied on receive (seconds). */
        public Builder visibilityTimeout(int seconds) {
            this.visibilityTimeout = seconds;
            return this;
        }

        /** Max messages per receive (default 10). */
        public Builder maxMessages(int max) {
            this.maxMessages = max;
            return this;
        }

        public Builder onError(ErrorHandler handler) {
            this.onError = handler;
            return this;
        }

        public Builder onUnknownUrn(UnknownUrnHandler handler) {
            this.onUnknownUrn = handler;
            return this;
        }

        public SqsConsumer build() {
            return new SqsConsumer(this);
        }
    }
}
