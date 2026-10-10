package com.babelqueue.sqs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.babelqueue.BabelQueueException;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import com.babelqueue.UnknownUrnException;
import com.babelqueue.UnknownUrnStrategy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

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
        assertTrue(sqs.deleted.isEmpty()); // released, never deleted
        // Default release: ChangeMessageVisibility(ReceiptHandle, 0) = redeliver now.
        assertEquals(1, sqs.visibilityChanges.size());
        ChangeMessageVisibilityRequest release = sqs.visibilityChanges.get(0);
        assertEquals(URL, release.queueUrl());
        assertEquals("rh-1", release.receiptHandle());
        assertEquals(0, release.visibilityTimeout());
    }

    @Test
    void throwingHandlerReleasesWithConfiguredBackoff() {
        FakeSqsClient sqs = new FakeSqsClient();
        sqs.seed(URL, EnvelopeCodec.encode(
            EnvelopeCodec.make("urn:babel:orders:created", Map.of("x", 1), "orders", null)), 2);

        SqsConsumer.builder(sqs, URL)
            .handler("urn:babel:orders:created", (e, m) -> { throw new IllegalStateException("boom"); })
            .releaseDelaySeconds(30)
            .build().poll();

        assertTrue(sqs.deleted.isEmpty());
        assertEquals(1, sqs.visibilityChanges.size());
        assertEquals("seed-1", sqs.visibilityChanges.get(0).receiptHandle());
        assertEquals(30, sqs.visibilityChanges.get(0).visibilityTimeout());
    }

    @Test
    void successfulHandlerNeverChangesVisibility() {
        FakeSqsClient sqs = new FakeSqsClient();
        SqsPublisher.create(sqs, URL).publish("urn:babel:orders:created", Map.of("x", 1));
        SqsConsumer.builder(sqs, URL).handler("urn:babel:orders:created", (e, m) -> { }).build().poll();
        assertTrue(sqs.visibilityChanges.isEmpty());
        assertEquals(1, sqs.deleted.size());
    }

    @Test
    void unknownUrnReleaseStrategyChangesVisibilityWithDelay() {
        FakeSqsClient sqs = new FakeSqsClient();
        sqs.seed(URL, EnvelopeCodec.encode(
            EnvelopeCodec.make("urn:babel:orders:created", Map.of("x", 1), "orders", null)), 1);

        String[] unknown = {""};
        SqsConsumer.builder(sqs, URL)
            .unknownUrnStrategy(UnknownUrnStrategy.RELEASE)
            .unknownUrnReleaseDelaySeconds(12)
            .onUnknownUrn((e, m) -> unknown[0] = EnvelopeCodec.urn(e))
            .build().poll();

        assertEquals("urn:babel:orders:created", unknown[0]);
        assertTrue(sqs.deleted.isEmpty());
        assertEquals(1, sqs.visibilityChanges.size());
        ChangeMessageVisibilityRequest release = sqs.visibilityChanges.get(0);
        assertEquals(URL, release.queueUrl());
        assertEquals("seed-1", release.receiptHandle());
        assertEquals(12, release.visibilityTimeout());
    }

    @Test
    void unknownUrnReleaseStrategyDefaultsToZeroSeconds() {
        FakeSqsClient sqs = new FakeSqsClient();
        sqs.seed(URL, EnvelopeCodec.encode(
            EnvelopeCodec.make("urn:babel:orders:created", Map.of("x", 1), "orders", null)), 1);
        SqsConsumer.builder(sqs, URL).unknownUrnStrategy(UnknownUrnStrategy.RELEASE).build().poll();
        assertEquals(0, sqs.visibilityChanges.get(0).visibilityTimeout());
    }

    @Test
    void unknownUrnDeleteAndDeadLetterStrategiesDelete() {
        for (String strategy : List.of(UnknownUrnStrategy.DELETE, UnknownUrnStrategy.DEAD_LETTER)) {
            FakeSqsClient sqs = new FakeSqsClient();
            sqs.seed(URL, EnvelopeCodec.encode(
                EnvelopeCodec.make("urn:babel:orders:created", Map.of("x", 1), "orders", null)), 1);
            SqsConsumer.builder(sqs, URL).unknownUrnStrategy(strategy).build().poll();
            assertEquals(1, sqs.deleted.size(), strategy);
            assertTrue(sqs.visibilityChanges.isEmpty(), strategy);
        }
    }

    @Test
    void unknownUrnFailStrategyLeavesMessageEvenWithCallback() {
        FakeSqsClient sqs = new FakeSqsClient();
        sqs.seed(URL, EnvelopeCodec.encode(
            EnvelopeCodec.make("urn:babel:orders:created", Map.of("x", 1), "orders", null)), 1);
        boolean[] called = {false};
        SqsConsumer.builder(sqs, URL)
            .unknownUrnStrategy(UnknownUrnStrategy.FAIL)
            .onUnknownUrn((e, m) -> called[0] = true)
            .build().poll();
        assertTrue(called[0]);
        assertTrue(sqs.deleted.isEmpty());
        assertTrue(sqs.visibilityChanges.isEmpty());
    }

    @Test
    void releaseDelaysAreClampedToSqsRange() {
        int[][] cases = {{-1, 0}, {43_201, 43_200}, {Integer.MAX_VALUE, 43_200}, {43_200, 43_200}, {7, 7}};
        for (int[] c : cases) {
            FakeSqsClient sqs = new FakeSqsClient();
            sqs.seed(URL, EnvelopeCodec.encode(
                EnvelopeCodec.make("urn:babel:orders:created", Map.of("x", 1), "orders", null)), 1);
            sqs.seed(URL, EnvelopeCodec.encode(
                EnvelopeCodec.make("urn:babel:orders:unknown", Map.of("x", 1), "orders", null)), 1);
            SqsConsumer.builder(sqs, URL)
                .handler("urn:babel:orders:created", (e, m) -> { throw new IllegalStateException("boom"); })
                .releaseDelaySeconds(c[0])
                .unknownUrnStrategy(UnknownUrnStrategy.RELEASE)
                .unknownUrnReleaseDelaySeconds(c[0])
                .build().poll();
            assertEquals(2, sqs.visibilityChanges.size());
            assertEquals(c[1], sqs.visibilityChanges.get(0).visibilityTimeout(), "handler release " + c[0]);
            assertEquals(c[1], sqs.visibilityChanges.get(1).visibilityTimeout(), "unknown-URN release " + c[0]);
        }
    }

    @Test
    void unknownStrategyNameIsRejected() {
        SqsConsumer.Builder builder = SqsConsumer.builder(new FakeSqsClient(), URL);
        assertThrows(IllegalArgumentException.class, () -> builder.unknownUrnStrategy("requeue"));
    }

    @Test
    void failedReleaseIsReportedAndNeverEscapesPoll() {
        FakeSqsClient sqs = new FakeSqsClient();
        RuntimeException stale = new IllegalStateException("ReceiptHandleIsInvalid");
        sqs.visibilityError = stale;
        sqs.seed(URL, EnvelopeCodec.encode(
            EnvelopeCodec.make("urn:babel:orders:created", Map.of("x", 1), "orders", null)), 1);
        sqs.seed(URL, EnvelopeCodec.encode(
            EnvelopeCodec.make("urn:babel:orders:created", Map.of("x", 2), "orders", null)), 1);

        List<Throwable> errors = new ArrayList<>();
        int[] handled = {0};
        int n = SqsConsumer.builder(sqs, URL)
            .handler("urn:babel:orders:created", (e, m) -> {
                handled[0]++;
                throw new IllegalArgumentException("handler boom");
            })
            .onError((err, env, msg) -> errors.add(err))
            .build().poll();

        assertEquals(2, n);
        assertEquals(2, handled[0]); // the batch continues past the failed release
        assertEquals(4, errors.size());
        // The handler error is reported first, then the release error.
        assertInstanceOf(IllegalArgumentException.class, errors.get(0));
        assertSame(stale, errors.get(1));
        assertInstanceOf(IllegalArgumentException.class, errors.get(2));
        assertSame(stale, errors.get(3));
        assertTrue(sqs.deleted.isEmpty());
    }

    @Test
    void failedUnknownUrnReleaseIsReportedAndNeverEscapesPoll() {
        FakeSqsClient sqs = new FakeSqsClient();
        RuntimeException throttled = new IllegalStateException("ThrottlingException");
        sqs.visibilityError = throttled;
        sqs.seed(URL, EnvelopeCodec.encode(
            EnvelopeCodec.make("urn:babel:orders:created", Map.of("x", 1), "orders", null)), 1);

        Throwable[] captured = {null};
        int n = SqsConsumer.builder(sqs, URL)
            .unknownUrnStrategy(UnknownUrnStrategy.RELEASE)
            .onError((err, env, msg) -> captured[0] = err)
            .build().poll();

        assertEquals(1, n);
        assertSame(throttled, captured[0]);
    }

    @Test
    void failedDeleteAfterSuccessfulHandlerIsReportedDistinctlyAndNotReleased() {
        FakeSqsClient sqs = new FakeSqsClient();
        RuntimeException throttled = new IllegalStateException("ThrottlingException");
        sqs.deleteError = throttled;
        sqs.seed(URL, EnvelopeCodec.encode(
            EnvelopeCodec.make("urn:babel:orders:created", Map.of("x", 1), "orders", null)), 1);
        sqs.seed(URL, EnvelopeCodec.encode(
            EnvelopeCodec.make("urn:babel:orders:created", Map.of("x", 2), "orders", null)), 1);

        List<Throwable> errors = new ArrayList<>();
        int[] handled = {0};
        int n = SqsConsumer.builder(sqs, URL)
            .handler("urn:babel:orders:created", (e, m) -> handled[0]++)
            .releaseDelaySeconds(0)
            .onError((err, env, msg) -> errors.add(err))
            .build().poll();

        assertEquals(2, n);
        assertEquals(2, handled[0]); // each handler ran exactly once; the batch continues
        assertEquals(2, errors.size());
        for (Throwable err : errors) {
            assertInstanceOf(SqsDeleteException.class, err);
            assertSame(throttled, err.getCause());
        }
        assertTrue(sqs.visibilityChanges.isEmpty()); // never released after success
        assertTrue(sqs.deleted.isEmpty());
    }

    @Test
    void failedUnknownUrnDeleteIsReportedAndNeverEscapesPoll() {
        FakeSqsClient sqs = new FakeSqsClient();
        RuntimeException stale = new IllegalStateException("ReceiptHandleIsInvalid");
        sqs.deleteError = stale;
        sqs.seed(URL, EnvelopeCodec.encode(
            EnvelopeCodec.make("urn:babel:orders:created", Map.of("x", 1), "orders", null)), 1);

        Throwable[] captured = {null};
        int n = SqsConsumer.builder(sqs, URL)
            .unknownUrnStrategy(UnknownUrnStrategy.DELETE)
            .onError((err, env, msg) -> captured[0] = err)
            .build().poll();

        assertEquals(1, n);
        assertInstanceOf(SqsDeleteException.class, captured[0]);
        assertSame(stale, captured[0].getCause());
        assertTrue(sqs.visibilityChanges.isEmpty());
    }

    @Test
    void reconcilePreservesUnknownKeys() {
        FakeSqsClient sqs = new FakeSqsClient();
        String body = "{\"job\":\"urn:babel:orders:created\",\"trace_id\":\"t\",\"data\":{},"
            + "\"meta\":{\"id\":\"m\",\"queue\":\"orders\",\"lang\":\"php\",\"schema_version\":1,"
            + "\"created_at\":1,\"vendor_flag\":true},\"attempts\":0,\"extra_top\":1}";
        sqs.seed(URL, body, 3);

        Envelope[] seen = {null};
        SqsConsumer.builder(sqs, URL)
            .handler("urn:babel:orders:created", (e, m) -> seen[0] = e)
            .build().poll();
        assertEquals(2, seen[0].attempts());
        assertEquals(1L, seen[0].extras().get("extra_top"));
        assertEquals(true, seen[0].meta().extras().get("vendor_flag"));
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

    // ---- §3.7 schema-version gate -------------------------------------------------------------

    private static void sendWithVersion(FakeSqsClient sqs, String body, String version) {
        SendMessageRequest.Builder request = SendMessageRequest.builder().queueUrl(URL).messageBody(body);
        if (version != null) {
            request.messageAttributes(Map.of("bq-schema-version",
                MessageAttributeValue.builder().dataType("Number").stringValue(version).build()));
        }
        sqs.sendMessage(request.build());
    }

    private static String validBody() {
        return EnvelopeCodec.encode(
            EnvelopeCodec.make("urn:babel:orders:created", Map.of("order_id", 7), "orders", null));
    }

    @Test
    void schemaVersionGateDecodesWhenAttributeMissingBlankOrSupported() {
        for (String version : new String[] {null, "", " \t", "\n\u000b\f\r", "1"}) {
            FakeSqsClient sqs = new FakeSqsClient();
            sendWithVersion(sqs, validBody(), version);

            int[] handled = {0};
            SqsConsumer.builder(sqs, URL)
                .handler("urn:babel:orders:created", (e, m) -> handled[0]++)
                .build().poll();

            assertEquals(1, handled[0], "version=" + version);
            assertEquals(1, sqs.deleted.size(), "version=" + version);
        }
    }

    @Test
    void schemaVersionGateRejectsWithoutDecodingOrHandlerAndLeavesMessage() {
        for (String version : new String[] {"2", "x"}) {
            FakeSqsClient sqs = new FakeSqsClient();
            // A non-JSON body proves the body is never decoded: decoding it would throw out of poll().
            sendWithVersion(sqs, "this is not json", version);

            int[] handled = {0};
            List<Envelope> envelopes = new ArrayList<>();
            List<Throwable> errors = new ArrayList<>();
            SqsConsumer.builder(sqs, URL)
                .handler("urn:babel:orders:created", (e, m) -> handled[0]++)
                .onError((err, env, msg) -> {
                    errors.add(err);
                    envelopes.add(env);
                })
                .build().poll();

            assertEquals(0, handled[0], "version=" + version);
            assertEquals(1, errors.size(), "version=" + version);
            assertInstanceOf(BabelQueueException.class, errors.get(0));
            assertNull(envelopes.get(0), "version=" + version);
            assertTrue(sqs.deleted.isEmpty(), "version=" + version);
            assertTrue(sqs.visibilityChanges.isEmpty(), "version=" + version);
        }
    }
}
