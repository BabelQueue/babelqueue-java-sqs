package com.babelqueue.sqs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

class SqsPublisherTest {

    private static final String URL = "https://sqs.eu-central-1.amazonaws.com/123456789012/orders";

    @Test
    void publishProjectsContractAttributes() {
        FakeSqsClient sqs = new FakeSqsClient();
        String id = SqsPublisher.create(sqs, URL).publish("urn:babel:orders:created", Map.of("order_id", 1042));

        assertEquals(1, sqs.sent.size());
        SendMessageRequest req = sqs.sent.get(0);
        assertEquals(URL, req.queueUrl());

        Envelope env = EnvelopeCodec.decode(req.messageBody());
        assertEquals("urn:babel:orders:created", env.job());
        assertEquals("orders", env.meta().queue()); // derived from the URL
        assertEquals(id, env.meta().id());

        Map<String, MessageAttributeValue> a = req.messageAttributes();
        assertEquals("urn:babel:orders:created", a.get("bq-job").stringValue());
        assertEquals("String", a.get("bq-job").dataType());
        assertEquals("1", a.get("bq-schema-version").stringValue());
        assertEquals("Number", a.get("bq-schema-version").dataType());
        assertEquals("java", a.get("bq-source-lang").stringValue());
        assertEquals(env.traceId(), a.get("bq-trace-id").stringValue());
        assertEquals(id, a.get("bq-message-id").stringValue());
        assertNotNull(a.get("bq-created-at").stringValue());
    }

    @Test
    void fifoSetsGroupAndDedup() {
        FakeSqsClient sqs = new FakeSqsClient();
        String url = "https://sqs.eu-central-1.amazonaws.com/123456789012/orders.fifo";
        String id = SqsPublisher.builder(sqs, url).fifo(true).build()
            .publish("urn:babel:orders:created", Map.of("x", 1));
        SendMessageRequest req = sqs.sent.get(0);
        assertEquals("orders.fifo", req.messageGroupId());
        assertEquals(id, req.messageDeduplicationId());
    }

    @Test
    void contentDedupOmitsDedupId() {
        FakeSqsClient sqs = new FakeSqsClient();
        String url = "https://sqs.eu-central-1.amazonaws.com/123456789012/orders.fifo";
        SqsPublisher.builder(sqs, url).fifo(true).contentDedup(true).messageGroupId("grp").build()
            .publish("urn:babel:orders:created", Map.of("x", 1));
        SendMessageRequest req = sqs.sent.get(0);
        assertEquals("grp", req.messageGroupId());
        assertNull(req.messageDeduplicationId());
    }

    @Test
    void clientErrorPropagates() {
        FakeSqsClient sqs = new FakeSqsClient(new RuntimeException("aws down"));
        RuntimeException e = assertThrows(RuntimeException.class,
            () -> SqsPublisher.create(sqs, URL).publish("urn:x:y", Map.of()));
        assertEquals("aws down", e.getMessage());
    }

    @Test
    void queueNameDerivation() {
        assertEquals("default", SqsQueues.nameFromUrl(null));
        assertEquals("orders", SqsQueues.nameFromUrl("https://sqs.x/123/orders"));
        assertEquals("default", SqsQueues.nameFromUrl("///"));
    }
}
