package com.babelqueue.sqs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.babelqueue.BabelQueueException;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import com.babelqueue.UnknownUrnException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SqsConsumerTest {

    private static final String URL = "https://sqs.eu-central-1.amazonaws.com/123456789012/orders";

    @Test
    void routesValidMessageThenDeletes() {
        FakeSqsClient sqs = new FakeSqsClient();
        SqsPublisher.create(sqs, URL).publish("urn:babel:orders:created", Map.of("order_id", 7));

        Envelope[] seen = {null};
        int n = SqsConsumer.builder(sqs, URL)
            .handler("urn:babel:orders:created", (env, msg) -> seen[0] = env)
            .build()
            .poll();

        assertEquals(1, n);
        assertEquals("urn:babel:orders:created", seen[0].job());
        assertEquals(7, ((Number) seen[0].data().get("order_id")).intValue());
        assertEquals(1, sqs.deleted.size());
    }

    @Test
    void reconcilesAttemptsFromReceiveCount() {
        FakeSqsClient sqs = new FakeSqsClient();
        Envelope env = EnvelopeCodec.make("urn:babel:orders:created", Map.of("x", 1), "orders", null);
        sqs.seed(URL, EnvelopeCodec.encode(env), 3); // 3rd delivery -> attempts 2

        int[] attempts = {-1};
        SqsConsumer.builder(sqs, URL)
            .handler("urn:babel:orders:created", (e, m) -> attempts[0] = e.attempts())
            .build().poll();
        assertEquals(2, attempts[0]);
    }

    @Test
    void neverLowersRuntimeAttempts() {
        FakeSqsClient sqs = new FakeSqsClient();
        Envelope base = EnvelopeCodec.make("urn:babel:orders:created", Map.of("x", 1), "orders", null);
        Envelope bumped = new Envelope(base.job(), base.traceId(), base.data(), base.meta(), 5, null);
        sqs.seed(URL, EnvelopeCodec.encode(bumped), 1);

        int[] attempts = {-1};
        SqsConsumer.builder(sqs, URL)
            .handler("urn:babel:orders:created", (e, m) -> attempts[0] = e.attempts())
            .build().poll();
        assertEquals(5, attempts[0]);
    }

    @Test
    void throwingHandlerLeavesMessageAndReportsOnError() {
        FakeSqsClient sqs = new FakeSqsClient();
        SqsPublisher.create(sqs, URL).publish("urn:babel:orders:created", Map.of("x", 1));

        Throwable[] captured = {null};
        SqsConsumer.builder(sqs, URL)
            .handler("urn:babel:orders:created", (e, m) -> { throw new IllegalStateException("boom"); })
            .onError((err, env, msg) -> captured[0] = err)
            .build().poll();

        assertInstanceOf(IllegalStateException.class, captured[0]);
        assertTrue(sqs.deleted.isEmpty()); // left for visibility-timeout redelivery
    }

    @Test
    void nonConformantEnvelopeReportsOnError() {
        FakeSqsClient sqs = new FakeSqsClient();
        sqs.seed(URL, "{\"not\":\"an envelope\"}", 1);

        Throwable[] captured = {null};
        SqsConsumer.builder(sqs, URL).onError((err, env, msg) -> captured[0] = err).build().poll();

        assertInstanceOf(BabelQueueException.class, captured[0]);
        assertTrue(sqs.deleted.isEmpty());
    }

    @Test
    void unknownUrnCallsHandlerThenDeletesOrReportsOnError() {
        Envelope env = EnvelopeCodec.make("urn:babel:orders:created", Map.of("x", 1), "orders", null);

        FakeSqsClient sqs1 = new FakeSqsClient();
        sqs1.seed(URL, EnvelopeCodec.encode(env), 1);
        String[] unknown = {""};
        SqsConsumer.builder(sqs1, URL)
            .onUnknownUrn((e, m) -> unknown[0] = EnvelopeCodec.urn(e))
            .build().poll();
        assertEquals("urn:babel:orders:created", unknown[0]);
        assertEquals(1, sqs1.deleted.size());

        FakeSqsClient sqs2 = new FakeSqsClient();
        sqs2.seed(URL, EnvelopeCodec.encode(env), 1);
        Throwable[] captured = {null};
        SqsConsumer.builder(sqs2, URL).onError((err, e, m) -> captured[0] = err).build().poll();
        assertInstanceOf(UnknownUrnException.class, captured[0]);
        assertTrue(sqs2.deleted.isEmpty());
    }

    @Test
    void pollPassesContractReceiveOptions() {
        FakeSqsClient sqs = new FakeSqsClient();
        SqsConsumer.builder(sqs, URL)
            .waitTimeSeconds(5).visibilityTimeout(45).maxMessages(3)
            .build().poll();

        assertEquals(5, sqs.lastReceive.waitTimeSeconds());
        assertEquals(45, sqs.lastReceive.visibilityTimeout());
        assertEquals(3, sqs.lastReceive.maxNumberOfMessages());
        assertEquals(List.of("All"), sqs.lastReceive.messageAttributeNames());
        assertEquals(List.of("ApproximateReceiveCount"), sqs.lastReceive.attributeNamesAsStrings());
    }

    @Test
    void runStopsImmediatelyWhenShouldNotContinue() {
        FakeSqsClient sqs = new FakeSqsClient();
        SqsConsumer.builder(sqs, URL).build().run(() -> false);
        assertNull(sqs.lastReceive); // never polled
    }

    @Test
    void runLoopsWhileShouldContinue() {
        FakeSqsClient sqs = new FakeSqsClient();
        boolean[] once = {true};
        SqsConsumer.builder(sqs, URL).build().run(() -> {
            if (once[0]) {
                once[0] = false;
                return true;
            }
            return false;
        });
        assertNotNull(sqs.lastReceive); // polled once
    }

    @Test
    void clientErrorPropagates() {
        FakeSqsClient sqs = new FakeSqsClient(new RuntimeException("aws down"));
        RuntimeException e = assertThrows(RuntimeException.class,
            () -> SqsConsumer.builder(sqs, URL).build().poll());
        assertEquals("aws down", e.getMessage());
    }
}
