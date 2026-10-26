package com.babelqueue.sqs;

import com.babelqueue.BabelQueueException;
import com.babelqueue.DeadLetters;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import com.babelqueue.PoisonRecord;
import com.babelqueue.UnknownUrnException;
import com.babelqueue.UnknownUrnStrategy;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BooleanSupplier;
import java.util.function.DoubleSupplier;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

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
 * {@code unknownUrnReleaseDelaySeconds} (default {@code 0}); {@code dead_letter} writes the envelope, annotated with
 * a {@code dead_letter} block (reason {@code unknown_urn}), to the {@link Builder#deadLetterQueueUrl DLQ queue} and
 * then deletes it, or degrades to {@code delete} when no DLQ queue is configured (the contract's "DLQ disabled" rule).
 * The poll loop never stops on a bad message, a failed release or a failed delete — observe via
 * {@code onError}/{@code onUnknownUrn}. Configure a queue {@code RedrivePolicy} so a message left to the broker
 * (see below), redelivered immediately under the default {@code 0} delay, ends in a native DLQ.
 *
 * <p><b>Poison and version gate (ADR-0037).</b> "Dead-lettering on" means a {@link Builder#deadLetterQueueUrl DLQ
 * queue URL} is configured (opt-in; a FIFO DLQ is {@code <queue>-dlq.fifo}). Only the <em>explicitly configured</em>
 * {@link Builder#unknownUrnStrategy strategy} is read by these two rules (none configured means {@code fail}); an
 * {@code onUnknownUrn} hook, and the {@code delete} it implies for an unknown URN, never affect them.
 * <ul>
 *   <li><b>Poison body</b> — not JSON, not an envelope, no valid identity or an unsupported
 *       {@code meta.schema_version}: never handed to a handler and never retried. {@code delete} deletes it; with a
 *       DLQ queue the ADR-0035 record ({@link PoisonRecord}, {@code reason: "poison"}) is sent to it and the message
 *       deleted <em>only after</em> the send succeeded (a failed send leaves it unsettled and is reported); with no
 *       DLQ queue it is reported and left to the broker's native mechanism (no {@code DeleteMessage}: visibility
 *       expiry redelivers it and the queue's {@code RedrivePolicy} moves it after {@code maxReceiveCount}).</li>
 *   <li><b>Version gate</b> (§3.7) — the {@code bq-schema-version} message attribute is checked <b>before</b> the
 *       body is decoded: when it is present and is not exactly the schema version the core supports
 *       ({@link EnvelopeCodec#SCHEMA_VERSION}, compared exactly as sent, with no trimming — so {@code "2"},
 *       {@code "x"}, {@code "01"} and {@code " 1"} are all unknown), the body is never decoded or routed, no handler
 *       or hook runs, and nothing is retried, on the first delivery, whatever {@code ApproximateReceiveCount} says.
 *       {@code onError} is notified (with a {@code null} envelope). {@code delete} deletes it; with a DLQ queue the
 *       body is sent to it <b>exactly as received</b> (no {@code dead_letter} block) and the message deleted only
 *       after the send succeeded (a failed send leaves it unsettled); with no DLQ queue it is left to the broker's
 *       native mechanism, as above. A missing or blank (empty or ASCII-whitespace-only: space, tab, LF, VT, FF, CR)
 *       attribute changes nothing, and a {@code "1"} attribute still goes through the post-decode
 *       {@link EnvelopeCodec#accepts} check (a body with an unsupported {@code meta.schema_version} is then poison).</li>
 * </ul>
 */
public final class SqsConsumer {

    private static final String SCHEMA_VERSION_ATTRIBUTE = "bq-schema-version";

    /**
     * Notified of a non-conformant envelope, an unmapped URN (no {@code onUnknownUrn}), a throwing
     * handler, a failed release ({@code ChangeMessageVisibility} error), a failed delete of a
     * processed message (reported as {@link SqsDeleteException}; the message is not released), or a
     * message rejected by the §3.7 schema-version gate. {@code envelope} is {@code null} when the body
     * was not decoded (a version-gated message) — guard against it before dereferencing.
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
    /** The strategy the caller set explicitly, or {@code null}: the only one the poison and version-gate rules read. */
    private final String configuredStrategy;
    private final String deadLetterQueueUrl;
    private final double retryBackoffSeconds;
    private final double retryBackoffMultiplier;
    private final double retryBackoffMaxSeconds;
    private final boolean retryJitterFull;
    private final DoubleSupplier random;
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
        this.configuredStrategy = builder.unknownUrnStrategy;
        this.deadLetterQueueUrl = builder.deadLetterQueueUrl;
        this.retryBackoffSeconds = builder.retryBackoffSeconds;
        this.retryBackoffMultiplier = builder.retryBackoffMultiplier;
        this.retryBackoffMaxSeconds = builder.retryBackoffMaxSeconds;
        this.retryJitterFull = builder.retryJitterFull;
        this.random = builder.random != null ? builder.random : () -> ThreadLocalRandom.current().nextDouble();
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
        String declaredVersion = rawSchemaVersion(message);
        if (declaredVersion != null && !isBlankSchemaVersion(declaredVersion)
            && !declaredVersion.equals(Integer.toString(EnvelopeCodec.SCHEMA_VERSION))) {
            settleVersionGated(message, declaredVersion);
            return;
        }

        String body = message.body() == null ? "" : message.body();
        Envelope envelope = reconcile(
            EnvelopeCodec.decode(body),
            message.attributesAsStrings().get("ApproximateReceiveCount"));

        if (!EnvelopeCodec.accepts(envelope)) {
            settlePoison(message, body, envelope);
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
            release(message, failureDelaySeconds(envelope), envelope);
            return;
        }
        // Outside the handler's try: a failed delete is not a handler failure, so it is
        // reported as SqsDeleteException and the message is never released.
        delete(message, envelope);
    }

    /**
     * Settles a version-gated delivery on its first delivery (ADR-0037 decision 1, broker-bindings §3.7): never
     * decoded, no handler or hook, no retry. Reads only the explicitly configured strategy.
     */
    private void settleVersionGated(Message message, String declaredVersion) {
        report(new BabelQueueException("Rejected a BabelQueue message with unsupported "
            + SCHEMA_VERSION_ATTRIBUTE + " attribute '" + declaredVersion + "' (supported: "
            + EnvelopeCodec.SCHEMA_VERSION + "); body not decoded."), null, message);
        if (UnknownUrnStrategy.DELETE.equals(configuredStrategy)) {
            delete(message, null);
            return;
        }
        if (deadLetterQueueUrl == null) {
            // Rule 3: nothing to write to. Neither deleted nor released: the broker's native mechanism
            // (visibility expiry, then the queue's RedrivePolicy) takes it. The SDK does not retry it.
            return;
        }
        // Rule 4: the raw body, byte for byte, then delete. Rule 5: a failed send leaves it unsettled.
        String body = message.body() == null ? "" : message.body();
        if (sendToDeadLetterQueue(message, body, message.hasMessageAttributes()
                ? message.messageAttributes() : Map.of(), null)) {
            delete(message, null);
        }
    }

    /**
     * Settles a poison body (ADR-0037 decision 2, error-handling.md §7): never handed to a handler, never retried.
     * Reads only the explicitly configured strategy.
     */
    private void settlePoison(Message message, String body, Envelope partial) {
        BabelQueueException cause = new BabelQueueException("Rejected a non-conformant BabelQueue envelope from SQS.");
        report(cause, partial, message);
        if (UnknownUrnStrategy.DELETE.equals(configuredStrategy)) {
            delete(message, partial);
            return;
        }
        if (deadLetterQueueUrl == null) {
            // Native: neither deleted nor released (visibility expiry, then the queue's RedrivePolicy).
            return;
        }
        String record;
        try {
            record = poisonRecord(body.getBytes(StandardCharsets.UTF_8), SqsQueues.nameFromUrl(queueUrl),
                receiveAttempts(message), cause);
        } catch (RuntimeException error) {
            report(error, partial, message);
            return;
        }
        if (sendToDeadLetterQueue(message, record, message.hasMessageAttributes()
                ? message.messageAttributes() : Map.of(), partial)) {
            delete(message, partial);
        }
    }

    /**
     * The ADR-0035 DLQ record of a poison {@code body}: the one builder this consumer writes with (conformance
     * {@code poison_dlq} drives it directly for bodies SQS itself cannot carry, such as invalid UTF-8).
     */
    static String poisonRecord(byte[] body, String originalQueue, int attempts, Throwable cause) {
        return PoisonRecord.build(body, originalQueue, attempts, cause.getMessage(), cause.getClass().getName());
    }

    /** {@code ApproximateReceiveCount - 1} (informational {@code dead_letter.attempts}), never negative. */
    private static int receiveAttempts(Message message) {
        String receiveCount = message.attributesAsStrings().get("ApproximateReceiveCount");
        if (receiveCount == null) {
            return 0;
        }
        try {
            return Math.max(Integer.parseInt(receiveCount.strip()) - 1, 0);
        } catch (NumberFormatException ex) {
            return 0;
        }
    }

    /**
     * Sends {@code body} to the DLQ queue; returns {@code false} (after reporting the failure) when the send failed,
     * so the caller leaves the message unsettled and SQS redelivers it once the visibility timeout expires. A FIFO
     * DLQ ({@code .fifo}) gets the consumer's queue name as the message group and the SQS message id (else a
     * digest of the body) as the deduplication id, so a send repeated after a lost delete is not duplicated.
     */
    private boolean sendToDeadLetterQueue(
            Message message, String body, Map<String, MessageAttributeValue> attributes, Envelope envelope) {
        try {
            SendMessageRequest.Builder request = SendMessageRequest.builder()
                .queueUrl(deadLetterQueueUrl)
                .messageBody(body);
            if (!attributes.isEmpty()) {
                request.messageAttributes(attributes);
            }
            if (deadLetterQueueUrl.endsWith(".fifo")) {
                request.messageGroupId(SqsQueues.nameFromUrl(queueUrl));
                request.messageDeduplicationId(message.messageId() != null && !message.messageId().isEmpty()
                    ? message.messageId()
                    : sha256Hex(body));
            }
            client.sendMessage(request.build());
            return true;
        } catch (RuntimeException sendError) {
            report(sendError, envelope, message);
            return false;
        }
    }

    private static String sha256Hex(String value) {
        try {
            return HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    /** The {@code bq-schema-version} attribute's string value exactly as sent (no trimming), or {@code null} when absent. */
    private static String rawSchemaVersion(Message message) {
        if (!message.hasMessageAttributes()) {
            return null;
        }
        MessageAttributeValue attribute = message.messageAttributes().get(SCHEMA_VERSION_ATTRIBUTE);
        return attribute == null ? null : attribute.stringValue();
    }

    /**
     * The shared cross-SDK "blank" definition for {@code bq-schema-version}: the empty string, or a value made up
     * <b>only</b> of ASCII whitespace ({@code ' '}, {@code \t}, {@code \n}, U+000B, {@code \f}, {@code \r}). Anything else
     * (NBSP, U+001C–U+001F, U+0085, U+FEFF, …) is <b>not</b> blank, unlike {@link String#isBlank()}.
     */
    private static boolean isBlankSchemaVersion(String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c != ' ' && (c < '\t' || c > '\r')) {
                return false;
            }
        }
        return true;
    }

    private void handleUnknownUrn(String urn, Envelope envelope, Message message) {
        if (onUnknownUrn != null) {
            onUnknownUrn.onUnknownUrn(envelope, message);
        }
        switch (unknownUrnStrategy) {
            case UnknownUrnStrategy.DELETE -> delete(message, envelope);
            case UnknownUrnStrategy.DEAD_LETTER -> deadLetterUnknownUrn(message, envelope);
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
     * The {@code dead_letter} unknown-URN strategy: with a DLQ queue the envelope, annotated with a {@code dead_letter}
     * block (reason {@code unknown_urn}), is sent to it and the message deleted only after the send succeeded (a
     * failed send leaves it unsettled and is reported); with none it degrades to {@code delete}.
     */
    private void deadLetterUnknownUrn(Message message, Envelope envelope) {
        if (deadLetterQueueUrl == null) {
            delete(message, envelope);
            return;
        }
        Envelope dead;
        String body;
        try {
            dead = DeadLetters.annotate(envelope, "unknown_urn", SqsQueues.nameFromUrl(queueUrl));
            body = EnvelopeCodec.encode(dead);
            if (sendToDeadLetterQueue(message, body, SqsAttributes.project(dead), envelope)) {
                delete(message, envelope);
            }
        } catch (RuntimeException error) {
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
        // withAttempts keeps every other component, including unknown top-level/meta keys.
        return envelope.withAttempts(reconciled);
    }

    /**
     * The visibility timeout (whole seconds) for the failure of {@code envelope}: the A9
     * {@code retry_backoff} schedule {@code d(n) = min(max, base * multiplier^(n-1))} with
     * {@code n = attempts + 1} (1-based; {@code attempts} is the SQS-reconciled count of earlier
     * receives), optional full jitter {@code uniform(0, d(n))}, rounded up to a whole second and
     * clamped to the SQS range 0-43200. With the defaults (multiplier 1, no cap, no jitter) this is
     * the constant {@code releaseDelaySeconds}, i.e. the pre-A9 behaviour.
     */
    private int failureDelaySeconds(Envelope envelope) {
        int n = Math.max(envelope.attempts(), 0) + 1;
        double d = retryBackoffSeconds;
        if (d > 0 && retryBackoffMultiplier != 1.0 && n > 1) {
            // A zero base stays zero: skipping pow() avoids 0 * Infinity = NaN for a huge n.
            d = retryBackoffSeconds * Math.pow(retryBackoffMultiplier, n - 1);
        }
        if (Double.isNaN(d) || d > MAX_VISIBILITY_TIMEOUT_SECONDS) {
            d = MAX_VISIBILITY_TIMEOUT_SECONDS;
        }
        if (retryBackoffMaxSeconds > 0 && d > retryBackoffMaxSeconds) {
            d = retryBackoffMaxSeconds;
        }
        if (retryJitterFull) {
            d = d * random.getAsDouble();
        }
        return clampSeconds((int) Math.ceil(d));
    }

    private static int clampSeconds(int seconds) {
        return Math.max(0, Math.min(seconds, MAX_VISIBILITY_TIMEOUT_SECONDS));
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
        private String deadLetterQueueUrl;
        private double retryBackoffSeconds;
        private double retryBackoffMultiplier = 1.0;
        private double retryBackoffMaxSeconds;
        private boolean retryJitterFull;
        private DoubleSupplier random;
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
            this.retryBackoffSeconds = clampDelay(seconds);
            return this;
        }

        /**
         * A9 {@code retry_backoff}: the base delay in seconds (default {@code 0}). The same knob as
         * {@link #releaseDelaySeconds(int)}, which stays as its alias; fractional values are rounded
         * up to a whole second on release (SQS visibility timeouts are integral).
         */
        public Builder retryBackoff(double seconds) {
            if (Double.isNaN(seconds) || Double.isInfinite(seconds) || seconds < 0) {
                throw new IllegalArgumentException("retry_backoff must be a finite number >= 0");
            }
            this.retryBackoffSeconds = Math.min(seconds, MAX_VISIBILITY_TIMEOUT_SECONDS);
            return this;
        }

        /** A9 {@code retry_backoff_multiplier} (default {@code 1}, must be {@code >= 1}). */
        public Builder retryBackoffMultiplier(double multiplier) {
            if (Double.isNaN(multiplier) || Double.isInfinite(multiplier) || multiplier < 1) {
                throw new IllegalArgumentException("retry_backoff_multiplier must be a finite number >= 1");
            }
            this.retryBackoffMultiplier = multiplier;
            return this;
        }

        /** A9 {@code retry_backoff_max} in seconds (default {@code 0} = unlimited). */
        public Builder retryBackoffMax(double seconds) {
            if (Double.isNaN(seconds) || Double.isInfinite(seconds) || seconds < 0) {
                throw new IllegalArgumentException("retry_backoff_max must be a finite number >= 0");
            }
            this.retryBackoffMaxSeconds = seconds;
            return this;
        }

        /** A9 {@code retry_jitter}: {@code "none"} (default) or {@code "full"}. */
        public Builder retryJitter(String jitter) {
            Objects.requireNonNull(jitter, "jitter");
            // Same normalisation as the core RetryBackoff.Jitter.parse (strip + case-insensitive); the core
            // class is not usable here until this module's core pin moves past 1.8.0.
            switch (jitter.strip().toLowerCase(java.util.Locale.ROOT)) {
                case "none" -> this.retryJitterFull = false;
                case "full" -> this.retryJitterFull = true;
                default -> throw new IllegalArgumentException("retry_jitter must be 'none' or 'full', got: " + jitter);
            }
            return this;
        }

        /** Test seam: the uniform [0,1) source used by full jitter. */
        Builder randomSource(DoubleSupplier source) {
            this.random = Objects.requireNonNull(source, "source");
            return this;
        }

        /**
         * Unknown-URN strategy: one of {@link UnknownUrnStrategy#FAIL}, {@link UnknownUrnStrategy#DELETE},
         * {@link UnknownUrnStrategy#RELEASE} or {@link UnknownUrnStrategy#DEAD_LETTER} (degrades to
         * {@code delete} when no {@link #deadLetterQueueUrl DLQ queue} is configured). Default: {@code fail}, or
         * {@code delete} for an unknown URN when only {@link #onUnknownUrn} is set (pre-1.2.0 behaviour; that implied
         * {@code delete} never reaches the poison and version-gate rules).
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
         * Turns BabelQueue dead-lettering on (opt-in, ADR-0037 decision 2): the URL of the {@code <queue>-dlq} queue
         * (an SQS name cannot contain a dot; a FIFO one is {@code <queue>-dlq.fifo}). A poison body is sent to it as the
         * ADR-0035 record, a version-gated delivery as its raw body, and an unknown URN under the
         * {@code dead_letter} strategy as an annotated envelope; the source message is deleted only after the send
         * succeeded. Unset (the default): a poison or version-gated delivery is left to the queue's native {@code RedrivePolicy}. The queue must
         * exist; this consumer never creates it.
         */
        public Builder deadLetterQueueUrl(String url) {
            if (url != null && url.isBlank()) {
                throw new IllegalArgumentException("deadLetterQueueUrl must not be blank");
            }
            this.deadLetterQueueUrl = url;
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
