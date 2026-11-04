package com.babelqueue.sqs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.babelqueue.BabelQueueException;
import com.babelqueue.DeadLetterEvent;
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
    // ---- ADR-0037: poison, version gate and the DLQ queue ---------------------------------------

    private static final String DLQ_URL = "https://sqs.eu-central-1.amazonaws.com/123456789012/orders.dlq";
    private static final String FIFO_DLQ_URL = "https://sqs.eu-central-1.amazonaws.com/123456789012/orders.dlq.fifo";

    private static SqsConsumer.Builder consumerWithDlq(FakeSqsClient sqs, List<Throwable> errors, int[] handled) {
        return SqsConsumer.builder(sqs, URL)
            .handler("urn:babel:orders:created", (e, m) -> handled[0]++)
            .onError((err, env, msg) -> errors.add(err))
            .deadLetterQueueUrl(DLQ_URL);
    }

    private static org.json.JSONObject sentJson(FakeSqsClient sqs, int index) {
        return new org.json.JSONObject(sqs.sent.get(index).messageBody());
    }

    @Test
    void poisonBodyWithDlqWritesTheAdr0035RecordThenDeletes() {
        FakeSqsClient sqs = new FakeSqsClient();
        sqs.seed(URL, "{\"not\":\"an envelope\",\"big\":12345678901234567890}", 4);
        List<Throwable> errors = new ArrayList<>();
        int[] handled = {0};

        consumerWithDlq(sqs, errors, handled).build().poll();

        assertEquals(0, handled[0]);
        assertEquals(1, sqs.sent.size());
        assertEquals(DLQ_URL, sqs.sent.get(0).queueUrl());
        // An object body is spliced: every original member stays as written, plus the dead_letter block.
        assertTrue(sqs.sent.get(0).messageBody().contains("\"big\":12345678901234567890"));
        org.json.JSONObject block = sentJson(sqs, 0).getJSONObject("dead_letter");
        assertEquals("poison", block.getString("reason"));
        assertEquals("orders", block.getString("original_queue"));
        assertEquals(3, block.getInt("attempts"));
        assertEquals(1, sqs.deleted.size());
        assertTrue(sqs.visibilityChanges.isEmpty());
        assertEquals(1, errors.size());
    }

    @Test
    void poisonNonJsonBodyIsWrappedUnderRaw() {
        FakeSqsClient sqs = new FakeSqsClient();
        sqs.seed(URL, "plain text, not JSON", 1);

        consumerWithDlq(sqs, new ArrayList<>(), new int[1]).build().poll();

        org.json.JSONObject record = sentJson(sqs, 0);
        assertEquals("plain text, not JSON", record.getString("raw"));
        assertEquals("poison", record.getJSONObject("dead_letter").getString("reason"));
        assertEquals(1, sqs.deleted.size());
    }

    @Test
    void poisonWithUnsupportedMetaSchemaVersionIsDeadLettered() {
        FakeSqsClient sqs = new FakeSqsClient();
        String body = validBody().replace("\"schema_version\":1", "\"schema_version\":2");
        sqs.seed(URL, body, 1);
        int[] handled = {0};

        consumerWithDlq(sqs, new ArrayList<>(), handled).build().poll();

        assertEquals(0, handled[0]);
        assertEquals("poison", sentJson(sqs, 0).getJSONObject("dead_letter").getString("reason"));
        assertEquals(1, sqs.deleted.size());
    }

    @Test
    void poisonFailedDlqWriteLeavesItUnsettledAndReports() {
        FakeSqsClient sqs = new FakeSqsClient();
        sqs.seed(URL, "{}", 1);
        sqs.sendError = new IllegalStateException("dlq down");
        List<Throwable> errors = new ArrayList<>();

        consumerWithDlq(sqs, errors, new int[1]).build().poll();

        assertTrue(sqs.deleted.isEmpty());
        assertTrue(sqs.visibilityChanges.isEmpty());
        assertEquals(2, errors.size());
        assertEquals("dlq down", errors.get(1).getMessage());
    }

    @Test
    void poisonWithoutDlqIsLeftToTheNativeMechanism() {
        FakeSqsClient sqs = new FakeSqsClient();
        sqs.seed(URL, "{}", 1);
        List<Throwable> errors = new ArrayList<>();

        SqsConsumer.builder(sqs, URL).onError((err, env, msg) -> errors.add(err)).build().poll();

        assertEquals(1, errors.size());
        assertTrue(sqs.sent.isEmpty());
        assertTrue(sqs.deleted.isEmpty());
        assertTrue(sqs.visibilityChanges.isEmpty());
    }

    @Test
    void poisonWithDeleteStrategyIsDeletedWithoutARecord() {
        FakeSqsClient sqs = new FakeSqsClient();
        sqs.seed(URL, "{}", 1);

        consumerWithDlq(sqs, new ArrayList<>(), new int[1]).unknownUrnStrategy(UnknownUrnStrategy.DELETE).build().poll();

        assertTrue(sqs.sent.isEmpty());
        assertEquals(1, sqs.deleted.size());
    }

    @Test
    void poisonWithDeadLetterStrategyButNoDlqQueueIsNative() {
        FakeSqsClient sqs = new FakeSqsClient();
        sqs.seed(URL, "{}", 1);

        SqsConsumer.builder(sqs, URL).unknownUrnStrategy(UnknownUrnStrategy.DEAD_LETTER).build().poll();

        assertTrue(sqs.sent.isEmpty());
        assertTrue(sqs.deleted.isEmpty());
    }

    @Test
    void poisonToAFifoDlqCarriesGroupAndDeduplicationIds() {
        FakeSqsClient sqs = new FakeSqsClient();
        sqs.seed(URL, "{}", 1);

        SqsConsumer.builder(sqs, URL).deadLetterQueueUrl(FIFO_DLQ_URL).build().poll();

        SendMessageRequest sent = sqs.sent.get(0);
        assertEquals(FIFO_DLQ_URL, sent.queueUrl());
        assertEquals("orders", sent.messageGroupId());
        assertNotNull(sent.messageDeduplicationId());
        assertTrue(sent.messageDeduplicationId().length() <= 128);
        assertEquals(1, sqs.deleted.size());
    }

    @Test
    void versionGatedWithDlqWritesTheRawBodyThenDeletes() {
        FakeSqsClient sqs = new FakeSqsClient();
        // Deliberately non-canonical: a writer that decodes and re-encodes would change these bytes.
        String raw = "{\"meta\": {\"schema_version\": 2}, \"job\": \"urn:babel:orders:created\", \"data\": {\"n\": 1.50}}\n";
        sendWithVersion(sqs, raw, "2");
        sqs.sent.clear();
        List<Throwable> errors = new ArrayList<>();
        int[] handled = {0};
        int[] unknown = {0};

        consumerWithDlq(sqs, errors, handled).onUnknownUrn((e, m) -> unknown[0]++).build().poll();

        assertEquals(0, handled[0]);
        assertEquals(0, unknown[0]);
        assertEquals(1, sqs.sent.size());
        assertEquals(DLQ_URL, sqs.sent.get(0).queueUrl());
        assertEquals(raw, sqs.sent.get(0).messageBody());
        assertTrue(!sqs.sent.get(0).messageBody().contains("dead_letter"));
        assertEquals(1, sqs.deleted.size());
        assertEquals(1, errors.size());
    }

    @Test
    void versionGatedIsSettledOnTheFirstDeliveryWhateverTheReceiveCountSays() {
        FakeSqsClient sqs = new FakeSqsClient();
        sqs.seed(URL, "body", 9, Map.of("bq-schema-version",
            MessageAttributeValue.builder().dataType("Number").stringValue("2").build()));

        consumerWithDlq(sqs, new ArrayList<>(), new int[1]).build().poll();

        assertEquals(1, sqs.sent.size());
        assertEquals(1, sqs.deleted.size());
    }

    @Test
    void versionGatedFailedDlqWriteLeavesItUnsettledAndReports() {
        FakeSqsClient sqs = new FakeSqsClient();
        sendWithVersion(sqs, "not json", "2");
        sqs.sendError = new IllegalStateException("dlq down");
        List<Throwable> errors = new ArrayList<>();

        consumerWithDlq(sqs, errors, new int[1]).build().poll();

        assertTrue(sqs.deleted.isEmpty());
        assertTrue(sqs.visibilityChanges.isEmpty());
        assertEquals(2, errors.size());
    }

    @Test
    void versionGatedWithDeleteStrategyIsDeletedWithoutARecord() {
        FakeSqsClient sqs = new FakeSqsClient();
        sendWithVersion(sqs, "not json", "2");
        sqs.sent.clear();

        consumerWithDlq(sqs, new ArrayList<>(), new int[1]).unknownUrnStrategy(UnknownUrnStrategy.DELETE).build().poll();

        assertTrue(sqs.sent.isEmpty());
        assertEquals(1, sqs.deleted.size());
    }

    @Test
    void versionGatedWithDeadLetterStrategyButNoDlqQueueIsNative() {
        FakeSqsClient sqs = new FakeSqsClient();
        sendWithVersion(sqs, "not json", "2");
        sqs.sent.clear();

        SqsConsumer.builder(sqs, URL).unknownUrnStrategy(UnknownUrnStrategy.DEAD_LETTER).build().poll();

        assertTrue(sqs.sent.isEmpty());
        assertTrue(sqs.deleted.isEmpty());
        assertTrue(sqs.visibilityChanges.isEmpty());
    }

    @Test
    void hookOnlyConsumerNeverDropsAVersionGatedDeliverySilently() {
        // ADR-0037 "Configured strategy only": the delete that onUnknownUrn implies for an unknown URN must not
        // reach the version gate. DLQ off -> native (not deleted); DLQ on -> raw record, and the hook never runs.
        int[] unknown = {0};

        FakeSqsClient off = new FakeSqsClient();
        sendWithVersion(off, validBody(), "2");
        off.sent.clear();
        SqsConsumer.builder(off, URL).onUnknownUrn((e, m) -> unknown[0]++).build().poll();
        assertTrue(off.deleted.isEmpty());
        assertTrue(off.sent.isEmpty());

        FakeSqsClient on = new FakeSqsClient();
        String body = validBody();
        sendWithVersion(on, body, "2");
        on.sent.clear();
        SqsConsumer.builder(on, URL).onUnknownUrn((e, m) -> unknown[0]++).deadLetterQueueUrl(DLQ_URL).build().poll();
        assertEquals(1, on.sent.size());
        assertEquals(body, on.sent.get(0).messageBody());
        assertEquals(1, on.deleted.size());
        assertEquals(0, unknown[0]);
    }

    @Test
    void hookOnlyConsumerNeverDropsAPoisonBodySilently() {
        FakeSqsClient off = new FakeSqsClient();
        off.seed(URL, "{}", 1);
        SqsConsumer.builder(off, URL).onUnknownUrn((e, m) -> { }).build().poll();
        assertTrue(off.deleted.isEmpty());

        FakeSqsClient on = new FakeSqsClient();
        on.seed(URL, "{}", 1);
        SqsConsumer.builder(on, URL).onUnknownUrn((e, m) -> { }).deadLetterQueueUrl(DLQ_URL).build().poll();
        assertEquals("poison", sentJson(on, 0).getJSONObject("dead_letter").getString("reason"));
        assertEquals(1, on.deleted.size());
    }

    @Test
    void unknownUrnDeadLetterStrategyWritesTheAnnotatedEnvelopeThenDeletes() {
        FakeSqsClient sqs = new FakeSqsClient();
        sqs.seed(URL, validBody(), 1);

        SqsConsumer.builder(sqs, URL).unknownUrnStrategy(UnknownUrnStrategy.DEAD_LETTER)
            .deadLetterQueueUrl(DLQ_URL).build().poll();

        assertEquals(1, sqs.sent.size());
        assertEquals("unknown_urn", sentJson(sqs, 0).getJSONObject("dead_letter").getString("reason"));
        assertEquals("urn:babel:orders:created", sentJson(sqs, 0).getString("job"));
        assertEquals(1, sqs.deleted.size());
    }

    @Test
    void unknownUrnDeadLetterFailedWriteLeavesItUnsettled() {
        FakeSqsClient sqs = new FakeSqsClient();
        sqs.seed(URL, validBody(), 1);
        sqs.sendError = new IllegalStateException("dlq down");
        List<Throwable> errors = new ArrayList<>();

        SqsConsumer.builder(sqs, URL).unknownUrnStrategy(UnknownUrnStrategy.DEAD_LETTER)
            .deadLetterQueueUrl(DLQ_URL).onError((err, env, msg) -> errors.add(err)).build().poll();

        assertTrue(sqs.deleted.isEmpty());
        assertEquals(1, errors.size());
    }

    @Test
    void blankDeadLetterQueueUrlIsRejected() {
        assertThrows(IllegalArgumentException.class,
            () -> SqsConsumer.builder(new FakeSqsClient(), URL).deadLetterQueueUrl(" "));
    }

    // ---- dead-letter observer (ADR-0039 section 5.1) -----------------------------------------

    private static final DeadLetterEvent POISON_EVENT = new DeadLetterEvent("orders", "orders.dlq", "poison");

    @Test
    void observerIsCalledOnceForAVersionGatedRawBodyWithAnEmptyReason() {
        FakeSqsClient sqs = new FakeSqsClient();
        sendWithVersion(sqs, "not json", "2");
        sqs.sent.clear();
        List<DeadLetterEvent> events = new ArrayList<>();

        consumerWithDlq(sqs, new ArrayList<>(), new int[1]).deadLetterObserver(events::add).build().poll();

        assertEquals(List.of(new DeadLetterEvent("orders", "orders.dlq", "")), events);
        assertEquals(1, sqs.sent.size());
        assertEquals(1, sqs.deleted.size());
    }

    @Test
    void observerIsCalledOnceForAPoisonBody() {
        FakeSqsClient sqs = new FakeSqsClient();
        sqs.seed(URL, "plain text, not JSON", 1);
        List<DeadLetterEvent> events = new ArrayList<>();

        consumerWithDlq(sqs, new ArrayList<>(), new int[1]).deadLetterObserver(events::add).build().poll();

        assertEquals(List.of(POISON_EVENT), events);
        assertEquals(1, sqs.deleted.size());
    }

    @Test
    void observerIsCalledOnceForAnUnknownUrnUnderTheDeadLetterStrategy() {
        FakeSqsClient sqs = new FakeSqsClient();
        sqs.seed(URL, validBody(), 1);
        List<DeadLetterEvent> events = new ArrayList<>();

        SqsConsumer.builder(sqs, URL).unknownUrnStrategy(UnknownUrnStrategy.DEAD_LETTER)
            .deadLetterQueueUrl(DLQ_URL).deadLetterObserver(events::add).build().poll();

        assertEquals(List.of(new DeadLetterEvent("orders", "orders.dlq", "unknown_urn")), events);
        assertEquals(1, sqs.deleted.size());
    }

    @Test
    void unknownUrnEventReportsMetaQueueElsePoisonAndGateReportTheConsumerQueue() {
        // meta.queue differs from the consumer's queue: the unknown-URN event carries meta.queue (ADR-0039 5.1).
        FakeSqsClient billing = new FakeSqsClient();
        billing.seed(URL, EnvelopeCodec.encode(
            EnvelopeCodec.make("urn:babel:orders:created", Map.of("order_id", 7), "billing", null)), 1);
        List<DeadLetterEvent> events = new ArrayList<>();
        SqsConsumer.builder(billing, URL).unknownUrnStrategy(UnknownUrnStrategy.DEAD_LETTER)
            .deadLetterQueueUrl(DLQ_URL).deadLetterObserver(events::add).build().poll();
        assertEquals(List.of(new DeadLetterEvent("billing", "orders.dlq", "unknown_urn")), events);

        // A poison body whose JSON carries a meta.queue is still non-conformant: the consumer's queue is reported.
        FakeSqsClient poison = new FakeSqsClient();
        poison.seed(URL, "{\"meta\":{\"queue\":\"billing\"}}", 1);
        sendWithVersion(poison, "not json", "2");
        events.clear();
        consumerWithDlq(poison, new ArrayList<>(), new int[1]).deadLetterObserver(events::add).build().poll();
        consumerWithDlq(poison, new ArrayList<>(), new int[1]).deadLetterObserver(events::add).build().poll();
        assertEquals(2, events.size());
        events.forEach(e -> assertEquals("orders", e.queue()));
    }

    @Test
    void observerSeesTheDlqWriteBeforeTheSourceMessageIsDeleted() {
        FakeSqsClient sqs = new FakeSqsClient();
        sqs.seed(URL, "{}", 1);
        int[] deletedAtCall = {-1};

        consumerWithDlq(sqs, new ArrayList<>(), new int[1])
            .deadLetterObserver(e -> deletedAtCall[0] = sqs.deleted.size()).build().poll();

        assertEquals(0, deletedAtCall[0]);
        assertEquals(1, sqs.deleted.size());
    }

    @Test
    void observerIsNotCalledWhenTheDlqWriteFails() {
        List<DeadLetterEvent> events = new ArrayList<>();

        FakeSqsClient poison = new FakeSqsClient();
        poison.seed(URL, "{}", 1);
        poison.sendError = new IllegalStateException("dlq down");
        consumerWithDlq(poison, new ArrayList<>(), new int[1]).deadLetterObserver(events::add).build().poll();

        FakeSqsClient gated = new FakeSqsClient();
        sendWithVersion(gated, "not json", "2");
        gated.sendError = new IllegalStateException("dlq down");
        consumerWithDlq(gated, new ArrayList<>(), new int[1]).deadLetterObserver(events::add).build().poll();

        FakeSqsClient unknown = new FakeSqsClient();
        unknown.seed(URL, validBody(), 1);
        unknown.sendError = new IllegalStateException("dlq down");
        SqsConsumer.builder(unknown, URL).unknownUrnStrategy(UnknownUrnStrategy.DEAD_LETTER)
            .deadLetterQueueUrl(DLQ_URL).deadLetterObserver(events::add).build().poll();

        assertTrue(events.isEmpty());
        assertTrue(poison.deleted.isEmpty());
        assertTrue(gated.deleted.isEmpty());
        assertTrue(unknown.deleted.isEmpty());
    }

    @Test
    void observerIsNotCalledWhenNothingIsWrittenToTheDlq() {
        List<DeadLetterEvent> events = new ArrayList<>();

        // No DLQ queue: poison and version-gated bodies are left to the native RedrivePolicy.
        FakeSqsClient native1 = new FakeSqsClient();
        native1.seed(URL, "{}", 1);
        SqsConsumer.builder(native1, URL).deadLetterObserver(events::add).build().poll();
        FakeSqsClient native2 = new FakeSqsClient();
        sendWithVersion(native2, "not json", "2");
        SqsConsumer.builder(native2, URL).deadLetterObserver(events::add).build().poll();

        // dead_letter strategy without a DLQ queue degrades to delete.
        FakeSqsClient degraded = new FakeSqsClient();
        degraded.seed(URL, validBody(), 1);
        SqsConsumer.builder(degraded, URL).unknownUrnStrategy(UnknownUrnStrategy.DEAD_LETTER)
            .deadLetterObserver(events::add).build().poll();

        // Explicit delete drops poison with no record, even with a DLQ queue configured.
        FakeSqsClient dropped = new FakeSqsClient();
        dropped.seed(URL, "{}", 1);
        consumerWithDlq(dropped, new ArrayList<>(), new int[1]).unknownUrnStrategy(UnknownUrnStrategy.DELETE)
            .deadLetterObserver(events::add).build().poll();

        assertTrue(events.isEmpty());
        assertEquals(1, degraded.deleted.size());
        assertEquals(1, dropped.deleted.size());
    }

    @Test
    void exhaustedRetriesAreLeftToNativeRedriveAndNeverCallTheObserver() {
        FakeSqsClient sqs = new FakeSqsClient();
        sqs.seed(URL, validBody(), 50);
        List<DeadLetterEvent> events = new ArrayList<>();

        SqsConsumer.builder(sqs, URL)
            .handler("urn:babel:orders:created", (e, m) -> { throw new IllegalStateException("boom"); })
            .deadLetterQueueUrl(DLQ_URL)
            .deadLetterObserver(events::add)
            .build().poll();

        assertTrue(events.isEmpty());
        assertTrue(sqs.sent.isEmpty());
        assertTrue(sqs.deleted.isEmpty());
        assertEquals(1, sqs.visibilityChanges.size());
    }

    @Test
    void aThrowingObserverNeverChangesTheSettleOutcome() {
        for (Throwable failure : new Throwable[] {new IllegalStateException("obs"), new AssertionError("obs")}) {
            FakeSqsClient poison = new FakeSqsClient();
            poison.seed(URL, "{}", 1);
            List<Throwable> errors = new ArrayList<>();
            consumerWithDlq(poison, errors, new int[1]).deadLetterObserver(e -> sneakyThrow(failure)).build().poll();
            assertEquals(1, poison.sent.size());
            assertEquals(1, poison.deleted.size());
            assertTrue(poison.visibilityChanges.isEmpty());
            assertEquals(1, errors.size());

            FakeSqsClient gated = new FakeSqsClient();
            sendWithVersion(gated, "not json", "2");
            gated.sent.clear();
            consumerWithDlq(gated, new ArrayList<>(), new int[1]).deadLetterObserver(e -> sneakyThrow(failure))
                .build().poll();
            assertEquals(1, gated.sent.size());
            assertEquals(1, gated.deleted.size());

            FakeSqsClient unknown = new FakeSqsClient();
            unknown.seed(URL, validBody(), 1);
            SqsConsumer.builder(unknown, URL).unknownUrnStrategy(UnknownUrnStrategy.DEAD_LETTER)
                .deadLetterQueueUrl(DLQ_URL).deadLetterObserver(e -> sneakyThrow(failure)).build().poll();
            assertEquals(1, unknown.sent.size());
            assertEquals(1, unknown.deleted.size());
        }
    }

    @Test
    void withoutAnObserverTheDlqPathIsUnchanged() {
        FakeSqsClient sqs = new FakeSqsClient();
        sqs.seed(URL, "{}", 1);

        consumerWithDlq(sqs, new ArrayList<>(), new int[1]).deadLetterObserver(null).build().poll();

        assertEquals(1, sqs.sent.size());
        assertEquals(1, sqs.deleted.size());
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(Throwable t) throws T {
        throw (T) t;
    }

    @Test
    void eventQueueFallbackTreatsOnlyTheAsciiWhitespaceSetAsBlank() {
        String json = EnvelopeCodec.encode(
            EnvelopeCodec.make("urn:babel:orders:created", Map.of("order_id", 7), "billing", null));
        // meta.queue as JSON text -> the queue the event must report. Only the ASCII whitespace set is blank (ADR-0031):
        // Unicode spaces and control characters (Character.isWhitespace) are real, non-blank names.
        String[][] cases = {
            {" \\t", "orders"}, {"", "orders"}, {"\\u000b\\f\\r", "orders"},
            {"\\u2003", "\u2003"}, {"\\u00a0", "\u00a0"}, {"\\u001c", "\u001c"}, {"\\u0085", "\u0085"},
        };
        for (String[] c : cases) {
            FakeSqsClient sqs = new FakeSqsClient();
            sqs.seed(URL, json.replace("\"queue\":\"billing\"", "\"queue\":\"" + c[0] + "\""), 1);
            List<DeadLetterEvent> events = new ArrayList<>();
            SqsConsumer.builder(sqs, URL).unknownUrnStrategy(UnknownUrnStrategy.DEAD_LETTER)
                .deadLetterQueueUrl(DLQ_URL).deadLetterObserver(events::add).build().poll();
            assertEquals(1, events.size(), c[0]);
            assertEquals(c[1], events.get(0).queue(), "meta.queue=" + c[0]);
        }
    }
}
