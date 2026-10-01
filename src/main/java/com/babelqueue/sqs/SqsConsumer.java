package com.babelqueue.sqs;

import com.babelqueue.BabelQueueException;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import com.babelqueue.UnknownUrnException;
import com.babelqueue.UnknownUrnStrategy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

/**
 * Polls an SQS queue, decodes and validates each message, routes it to the handler
 * registered for its URN, and deletes it on success. A throwing handler's message is
 * <em>released</em>, never deleted: per broker-bindings.md §3 the consumer calls
 * {@code ChangeMessageVisibility(ReceiptHandle, VisibilityTimeout = releaseDelaySeconds)}
 * ({@code 0} = redeliver now (default), {@code N} = backoff N seconds, clamped to 12 h) and SQS
 * redelivers it (at-least-once). {@code attempts} is reconciled to
 * {@code ApproximateReceiveCount - 1} for the handler; SQS owns the counter.
 *
 * <p>A URN with no handler follows the {@link UnknownUrnStrategy unknown-URN strategy}:
 * {@code fail} reports {@link UnknownUrnException} and leaves the message (visibility expiry
 * redelivers it); {@code delete} deletes it; {@code release} releases it with
 * {@code unknownUrnReleaseDelaySeconds} (default {@code 0}); {@code dead_letter} degrades to {@code delete}
 * because this transport has no DLQ publisher (the contract's "DLQ disabled" rule).
 * The poll loop never stops on a bad message, a failed release or a failed delete — observe via
 * {@code onError}/{@code onUnknownUrn}. Configure a queue {@code RedrivePolicy} so a poison
 * message, redelivered immediately under the default {@code 0} delay, ends in a native DLQ.
 */
public final class SqsConsumer {

    /**
     * Notified of a non-conformant envelope, an unmapped URN (no {@code onUnknownUrn}), a throwing
     * handler, a failed release ({@code ChangeMessageVisibility} error), or a failed delete of a
     * processed message (reported as {@link SqsDeleteException}; the message is not released).
     */
    @FunctionalInterface
    public interface ErrorHandler {
        void onError(Throwable error, Envelope envelope, Message message);
    }

    /**
     * Called when a URN has no handler, before the unknown-URN strategy is applied. Setting
     * it without an explicit strategy keeps the pre-1.2.0 behaviour: the message is deleted.
     */
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
    private final String unknownUrnStrategy;
    private final int releaseDelaySeconds;
    private final int unknownUrnReleaseDelaySeconds;

    /** The SQS maximum for {@code VisibilityTimeout}: 12 hours, in seconds. */
    public static final int MAX_VISIBILITY_TIMEOUT_SECONDS = 43_200;

    private SqsConsumer(Builder builder) {
        this.client = builder.client;
        this.queueUrl = builder.queueUrl;
        this.handlers = Map.copyOf(builder.handlers);
        this.waitTimeSeconds = builder.waitTimeSeconds;
        this.visibilityTimeout = builder.visibilityTimeout;
        this.maxMessages = builder.maxMessages;
        this.onError = builder.onError;
        this.onUnknownUrn = builder.onUnknownUrn;
        this.unknownUrnStrategy = builder.unknownUrnStrategy != null
            ? builder.unknownUrnStrategy
            : (builder.onUnknownUrn != null ? UnknownUrnStrategy.DELETE : UnknownUrnStrategy.FAIL);
        this.releaseDelaySeconds = builder.releaseDelaySeconds;
        this.unknownUrnReleaseDelaySeconds = builder.unknownUrnReleaseDelaySeconds;
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
            handleUnknownUrn(urn, envelope, message);
            return;
        }

        try {
            handler.handle(envelope, message);
        } catch (Exception error) {
            // Never delete on failure: report first, then release with the backoff so SQS
            // redelivers it. A failed release is reported too and never escapes the poll loop.
            report(error, envelope, message);
            release(message, releaseDelaySeconds, envelope);
            return;
        }
        // Outside the handler's try: a failed delete is not a handler failure, so it is
        // reported as SqsDeleteException and the message is never released.
        delete(message, envelope);
    }

    private void handleUnknownUrn(String urn, Envelope envelope, Message message) {
        if (onUnknownUrn != null) {
            onUnknownUrn.onUnknownUrn(envelope, message);
        }
        switch (unknownUrnStrategy) {
            case UnknownUrnStrategy.DELETE, UnknownUrnStrategy.DEAD_LETTER -> delete(message, envelope);
            case UnknownUrnStrategy.RELEASE -> release(message, unknownUrnReleaseDelaySeconds, envelope);
            default -> {
                // fail: leave the message; visibility expiry redelivers it, native redrive quarantines it.
                if (onUnknownUrn == null) {
                    report(new UnknownUrnException(urn), envelope, message);
                }
            }
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
        // withAttempts keeps every other component, including unknown top-level/meta keys.
        return envelope.withAttempts(reconciled);
    }

    /**
     * Release (retry): {@code ChangeMessageVisibility(ReceiptHandle, delaySeconds)} — never a delete.
     * A broker error (stale receipt handle, throttling, network) is reported via {@code onError}
     * and swallowed: the message is still redelivered once its visibility timeout expires, and
     * the poll loop keeps running.
     */
    private void release(Message message, int delaySeconds, Envelope envelope) {
        if (message.receiptHandle() == null) {
            return;
        }
        try {
            client.changeMessageVisibility(ChangeMessageVisibilityRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .visibilityTimeout(delaySeconds)
                .build());
        } catch (RuntimeException releaseError) {
            report(releaseError, envelope, message);
        }
    }

    /**
     * Acknowledge a processed message with {@code DeleteMessage}. A broker error (throttling,
     * network, stale receipt handle) is reported via {@code onError} as an
     * {@link SqsDeleteException} and swallowed — no release, so the message comes back only
     * after its visibility timeout — and the poll loop keeps running.
     */
    private void delete(Message message, Envelope envelope) {
        if (message.receiptHandle() == null) {
            return;
        }
        try {
            client.deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build());
        } catch (RuntimeException deleteError) {
            report(new SqsDeleteException(deleteError), envelope, message);
        }
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
        private String unknownUrnStrategy;
        private int releaseDelaySeconds;
        private int unknownUrnReleaseDelaySeconds;

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

        /**
         * Backoff (seconds) applied when a handler throws: the message is released via
         * {@code ChangeMessageVisibility} with this {@code VisibilityTimeout}. Default {@code 0}
         * (redeliver now). Out-of-range values are clamped to 0–43200 (the SQS 12 h cap), like
         * the Go and Python transports.
         *
         * <p>With the default {@code 0} a permanently failing (poison) message is redelivered
         * immediately on every receive; configure a {@code RedrivePolicy}
         * ({@code maxReceiveCount}) on the queue so SQS moves it to a native DLQ, and/or set a
         * non-zero delay here.
         */
        public Builder releaseDelaySeconds(int seconds) {
            this.releaseDelaySeconds = clampDelay(seconds);
            return this;
        }

        /**
         * Unknown-URN strategy: one of {@link UnknownUrnStrategy#FAIL}, {@link UnknownUrnStrategy#DELETE},
         * {@link UnknownUrnStrategy#RELEASE} or {@link UnknownUrnStrategy#DEAD_LETTER} (degrades to
         * {@code delete}: no DLQ publisher in this transport). Default: {@code fail}, or {@code delete}
         * when only {@link #onUnknownUrn} is set (pre-1.2.0 behaviour).
         */
        public Builder unknownUrnStrategy(String strategy) {
            Objects.requireNonNull(strategy, "strategy");
            switch (strategy) {
                case UnknownUrnStrategy.FAIL, UnknownUrnStrategy.DELETE,
                    UnknownUrnStrategy.RELEASE, UnknownUrnStrategy.DEAD_LETTER -> this.unknownUrnStrategy = strategy;
                default -> throw new IllegalArgumentException("Unknown unknown-URN strategy: " + strategy);
            }
            return this;
        }

        /**
         * Backoff (seconds) for the {@code release} unknown-URN strategy. Default {@code 0}
         * (redeliver now, matching the Python and Node transports); out-of-range values are
         * clamped to 0–43200.
         */
        public Builder unknownUrnReleaseDelaySeconds(int seconds) {
            this.unknownUrnReleaseDelaySeconds = clampDelay(seconds);
            return this;
        }

        /** Clamp a delay to the SQS {@code VisibilityTimeout} range, 0–43200 s (Go/Python parity). */
        private static int clampDelay(int seconds) {
            return Math.max(0, Math.min(seconds, MAX_VISIBILITY_TIMEOUT_SECONDS));
        }

        public SqsConsumer build() {
            return new SqsConsumer(this);
        }
    }
}
