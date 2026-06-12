package com.babelqueue.sqs;

import com.babelqueue.Envelope;
import java.util.LinkedHashMap;
import java.util.Map;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

/**
 * Projects the envelope's contract fields onto native SQS {@code MessageAttributes} —
 * a redundant, routable view of the body (the body stays authoritative). Contract §3.2.
 */
final class SqsAttributes {

    private SqsAttributes() {}

    static Map<String, MessageAttributeValue> project(Envelope envelope) {
        Map<String, MessageAttributeValue> attrs = new LinkedHashMap<>();
        putString(attrs, "bq-job", envelope.job());
        putString(attrs, "bq-trace-id", envelope.traceId());
        if (envelope.meta() != null) {
            putString(attrs, "bq-message-id", envelope.meta().id());
            attrs.put("bq-schema-version", number(Integer.toString(envelope.meta().schemaVersion())));
            putString(attrs, "bq-source-lang", envelope.meta().lang());
            attrs.put("bq-created-at", number(Long.toString(envelope.meta().createdAt())));
        }
        return attrs;
    }

    private static void putString(Map<String, MessageAttributeValue> attrs, String key, String value) {
        if (value != null && !value.isEmpty()) {
            attrs.put(key, MessageAttributeValue.builder().dataType("String").stringValue(value).build());
        }
    }

    private static MessageAttributeValue number(String value) {
        return MessageAttributeValue.builder().dataType("Number").stringValue(value).build();
    }
}
