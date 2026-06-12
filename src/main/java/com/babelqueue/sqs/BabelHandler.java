package com.babelqueue.sqs;

import com.babelqueue.Envelope;
import software.amazon.awssdk.services.sqs.model.Message;

/** Processes one decoded, validated envelope and the raw SQS message it arrived on. */
@FunctionalInterface
public interface BabelHandler {

    /**
     * Handle a message. Returning normally acknowledges it (the consumer deletes it);
     * throwing leaves it for SQS to redeliver after the visibility timeout.
     */
    void handle(Envelope envelope, Message message) throws Exception;
}
