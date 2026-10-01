package com.babelqueue.sqs;

import com.babelqueue.BabelQueueException;

/**
 * Reported via {@link SqsConsumer.ErrorHandler#onError} when {@code DeleteMessage} fails for a
 * message whose processing already succeeded (a handler returned normally, or the unknown-URN
 * strategy chose {@code delete}). The broker error is the {@linkplain #getCause() cause}.
 *
 * <p>It is distinct from a handler failure: the message is <em>not</em> released, so it is
 * redelivered only after its visibility timeout expires (at-least-once; dedupe on
 * {@code meta.id} with the idempotency helper if the side effect must not repeat).
 */
public final class SqsDeleteException extends BabelQueueException {

    private static final long serialVersionUID = 1L;

    public SqsDeleteException(Throwable cause) {
        super("Failed to delete a processed SQS message; it will be redelivered after its visibility timeout.",
            cause);
    }
}
