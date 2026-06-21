package com.babelqueue.sqs;

import com.babelqueue.Envelope;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

/**
 * Projects the envelope's contract fields onto native SQS {@code MessageAttributes} —
 * a redundant, routable view of the body (the body stays authoritative). Contract §3.2.
 *
 * <p>It also carries out-of-band transport headers (e.g. the W3C {@code traceparent},
 * ADR-0028) <b>beside</b> the contract {@code bq-*} attributes on the same channel
 * {@code bq-trace-id} already rides — never inside the frozen envelope (GR-1). The
 * contract projection always wins a key collision, and the merge stops at the SQS
 * 10-attribute ceiling so unbounded out-of-band headers can never crowd out the
 * contract attributes.
 */
final class SqsAttributes {

    /** The SQS per-message cap on user {@code MessageAttributes}. */
    static final int MAX_ATTRIBUTES = 10;

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

    /**
     * Projects the contract attributes and overlays the out-of-band string {@code headers}
     * beside them — for {@link SqsPublisher#publishWithHeaders}. A blank key or value is
     * skipped; a contract {@code bq-*} key already present always wins (the header is
     * dropped, never clobbering it); and once the message reaches {@link #MAX_ATTRIBUTES}
     * no further header is added. Keys are merged in sorted order so the bounded subset is
     * deterministic. With a {@code null}/empty map this is byte-identical to {@link #project}.
     */
    static Map<String, MessageAttributeValue> projectWithHeaders(
        Envelope envelope, Map<String, String> headers) {
        Map<String, MessageAttributeValue> attrs = project(envelope);
        if (headers == null || headers.isEmpty()) {
            return attrs;
        }
        List<String> keys = new ArrayList<>(headers.keySet());
        keys.sort(String::compareTo);
        for (String key : keys) {
            if (attrs.size() >= MAX_ATTRIBUTES) {
                break; // respect the SQS 10-attribute ceiling
            }
            if (key == null || key.isEmpty() || attrs.containsKey(key)) {
                continue; // contract attribute wins a collision; skip blank keys
            }
            String value = headers.get(key);
            if (value == null || value.isEmpty()) {
                continue;
            }
            attrs.put(key, MessageAttributeValue.builder().dataType("String").stringValue(value).build());
        }
        return attrs;
    }

    /**
     * Surfaces a delivered message's {@code MessageAttributes} as a flat
     * {@code Map<String, String>} (the consume-side counterpart of
     * {@link #projectWithHeaders}), reading each attribute's {@code stringValue}. Both the
     * contract {@code bq-*} attributes and out-of-band headers (e.g. {@code traceparent})
     * surface; the consumer picks the keys it needs. Blank values are dropped; an empty or
     * {@code null} input yields an empty map.
     */
    static Map<String, String> extract(Map<String, MessageAttributeValue> attrs) {
        Map<String, String> out = new LinkedHashMap<>();
        if (attrs == null) {
            return out;
        }
        for (Map.Entry<String, MessageAttributeValue> e : attrs.entrySet()) {
            MessageAttributeValue v = e.getValue();
            String value = v == null ? null : v.stringValue();
            if (value != null && !value.isEmpty()) {
                out.put(e.getKey(), value);
            }
        }
        return out;
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
