package com.babelqueue.sqs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * Amazon SQS binding conformance against the vendored canonical suite's {@code sqs}
 * block: the §3 attribute projection and the {@code attempts = ApproximateReceiveCount - 1}
 * reconciliation, plus the ADR-0037 {@code version_gate_sqs} settle rule and the ADR-0035
 * {@code poison_dlq} record shape driven through this consumer. No AWS, no network.
 */
class SqsConformanceTest {

    private static final String URL = "https://sqs.eu-central-1.amazonaws.com/123456789012/orders";

    private static String resource(String path) throws Exception {
        try (InputStream in = SqsConformanceTest.class.getResourceAsStream("/conformance/" + path)) {
            if (in == null) {
                throw new IllegalStateException("vendored conformance resource missing: " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static JSONObject sqsBlock() throws Exception {
        return new JSONObject(resource("manifest.json")).getJSONObject("sqs");
    }

    @Test
    void attributeProjectionMatchesGolden() throws Exception {
        JSONObject projection = sqsBlock().getJSONObject("attribute_projection");
        Envelope envelope = EnvelopeCodec.decode(resource(projection.getString("envelope_file")));
        Map<String, MessageAttributeValue> got = SqsAttributes.project(envelope);
        JSONObject want = projection.getJSONObject("message_attributes");

        assertEquals(want.keySet(), got.keySet());
        for (String key : want.keySet()) {
            JSONObject expected = want.getJSONObject(key);
            assertEquals(expected.getString("DataType"), got.get(key).dataType(), key);
            assertEquals(expected.getString("StringValue"), got.get(key).stringValue(), key);
        }
    }

    @Test
    void attemptsReconciliationMatchesGolden() throws Exception {
        JSONArray cases = sqsBlock().getJSONObject("attempts_reconciliation").getJSONArray("cases");
        for (int i = 0; i < cases.length(); i++) {
            JSONObject testCase = cases.getJSONObject(i);
            Envelope base = EnvelopeCodec.make("urn:babel:orders:created", Map.of("x", 1), "orders", null);
            Envelope bumped = new Envelope(
                base.job(), base.traceId(), base.data(), base.meta(), testCase.getInt("body_attempts"), null);

            FakeSqsClient sqs = new FakeSqsClient();
            String receiveCount = testCase.isNull("approximate_receive_count")
                ? null
                : testCase.getString("approximate_receive_count");
            sqs.seed(URL, EnvelopeCodec.encode(bumped), receiveCount);

            int[] seen = {-1};
            SqsConsumer.builder(sqs, URL)
                .handler("urn:babel:orders:created", (env, message) -> seen[0] = env.attempts())
                .build()
                .poll();

            assertEquals(testCase.getInt("expected_attempts"), seen[0], testCase.getString("name"));
        }
    }

    @Test
    void schemaVersionGateMatchesGolden() throws Exception {
        JSONObject gate = sqsBlock().getJSONObject("schema_version_gate");
        String property = gate.getString("property");
        String fixture = resource(gate.getString("fixture"));
        JSONArray cases = gate.getJSONArray("cases");
        assertTrue(cases.length() > 0);

        for (int i = 0; i < cases.length(); i++) {
            JSONObject testCase = cases.getJSONObject(i);
            String label = testCase.has("value") ? "value=" + testCase.get("value") : "absent";

            SendMessageRequest.Builder request = SendMessageRequest.builder().queueUrl(URL).messageBody(fixture);
            if (!testCase.optBoolean("absent", false)) {
                request.messageAttributes(Map.of(property,
                    MessageAttributeValue.builder().dataType("Number").stringValue(testCase.getString("value")).build()));
            }
            FakeSqsClient sqs = new FakeSqsClient();
            sqs.sendMessage(request.build());

            List<String> handled = new ArrayList<>();
            SqsConsumer.builder(sqs, URL)
                .handler("urn:babel:orders:created", (env, message) -> handled.add(env.job()))
                .build()
                .poll();

            boolean decode = "decode".equals(testCase.getString("expect"));
            assertEquals(decode ? 1 : 0, handled.size(), label);
            assertEquals(decode ? 1 : 0, sqs.deleted.size(), label);
        }
    }
    // ---- poison_dlq (ADR-0035 shape, ADR-0037 decision 2) -----------------------------------------

    private static final String DLQ_URL = "https://sqs.eu-central-1.amazonaws.com/123456789012/orders.dlq";

    @Test
    void poisonDlqRecordShapeMatchesGolden() throws Exception {
        JSONArray cases = new JSONObject(resource("manifest.json")).getJSONObject("poison_dlq").getJSONArray("cases");
        assertTrue(cases.length() > 0);
        for (int i = 0; i < cases.length(); i++) {
            JSONObject testCase = cases.getJSONObject(i);
            String record;
            if (testCase.has("body_base64")) {
                // SQS delivers text, so a body that is not valid UTF-8 never reaches the consumer: drive the builder it
                // writes with directly.
                record = SqsConsumer.poisonRecord(PoisonRecordChecks.bodyOf(testCase), "orders", 0,
                    new com.babelqueue.BabelQueueException("poison"));
            } else {
                FakeSqsClient sqs = new FakeSqsClient();
                sqs.seed(URL, testCase.getString("body"), 1);
                int[] handled = {0};
                SqsConsumer.builder(sqs, URL)
                    .handler("urn:babel:orders:created", (env, message) -> handled[0]++)
                    .deadLetterQueueUrl(DLQ_URL)
                    .build()
                    .poll();
                assertEquals(0, handled[0], testCase.getString("name"));
                assertEquals(1, sqs.sent.size(), testCase.getString("name"));
                assertEquals(1, sqs.deleted.size(), testCase.getString("name"));
                record = sqs.sent.get(0).messageBody();
            }
            PoisonRecordChecks.check(testCase, record);
        }
    }

    // ---- version_gate_sqs (ADR-0037 decision 1) ---------------------------------------------------

    @Test
    void versionGateSettleRuleMatchesGolden() throws Exception {
        JSONObject section = new JSONObject(resource("manifest.json")).getJSONObject("version_gate_sqs");
        JSONArray cases = section.getJSONArray("cases");
        JSONArray bodies = section.getJSONArray("incoming_bodies");
        assertEquals(17, cases.length());
        for (int i = 0; i < cases.length(); i++) {
            JSONObject testCase = cases.getJSONObject(i);
            for (int b = 0; b < bodies.length(); b++) {
                runVersionGateCase(testCase, bodies.getJSONObject(b).getString("body"));
            }
        }
    }

    private static void runVersionGateCase(JSONObject testCase, String body) {
        String label = testCase.getString("name");
        boolean deadLetter = "on".equals(testCase.getString("dead_letter"));
        JSONObject expect = testCase.getJSONObject("expect");

        FakeSqsClient sqs = new FakeSqsClient();
        Map<String, MessageAttributeValue> attributes = new java.util.HashMap<>();
        attributes.put("bq-schema-version", MessageAttributeValue.builder().dataType("Number").stringValue("2").build());
        if (testCase.has("bq_attempts")) {
            attributes.put("bq-attempts", MessageAttributeValue.builder().dataType("Number")
                .stringValue(testCase.getString("bq_attempts")).build());
        }
        // A high receive count: the rule applies on any delivery without retry, whatever SQS counts.
        sqs.seed(URL, body, 9, attributes);
        if ("fails".equals(testCase.optString("dlq_write", "ok"))) {
            sqs.sendError = new IllegalStateException("dlq write failed");
        }
        int[] handled = {0};
        java.util.List<Throwable> errors = new ArrayList<>();
        SqsConsumer.Builder builder = SqsConsumer.builder(sqs, URL)
            .handler("urn:babel:orders:created", (env, message) -> handled[0]++)
            .onError((err, env, msg) -> errors.add(err));
        if (testCase.has("on_unknown_urn")) {
            builder.unknownUrnStrategy(testCase.getString("on_unknown_urn"));
        }
        if (deadLetter) {
            builder.deadLetterQueueUrl(DLQ_URL);
        }
        builder.build().poll();

        assertEquals(0, handled[0], label);
        // "off" means no DLQ writer exists in this consumer (no queue URL): outcome_no_writer applies.
        String outcome = !deadLetter && expect.has("outcome_no_writer")
            ? expect.getString("outcome_no_writer") : expect.getString("outcome");
        assertTrue(sqs.visibilityChanges.isEmpty(), label + ": a version-gated delivery is never released");
        switch (outcome) {
            case "dlq_raw" -> {
                assertEquals(1, sqs.sent.size(), label);
                assertEquals(DLQ_URL, sqs.sent.get(0).queueUrl(), label);
                assertEquals(body, sqs.sent.get(0).messageBody(), label);
                assertTrue(!sqs.sent.get(0).messageBody().contains("\"dead_letter\""), label);
                assertEquals(1, sqs.deleted.size(), label);
            }
            case "ack" -> {
                assertTrue(sqs.sent.isEmpty(), label);
                assertEquals(1, sqs.deleted.size(), label);
            }
            case "unsettled" -> {
                assertTrue(sqs.sent.isEmpty(), label);
                assertTrue(sqs.deleted.isEmpty(), label);
                // The write failure is reported on top of the gate rejection itself.
                assertTrue(errors.size() >= 2, label + ": the failed write must be reported");
            }
            case "native" -> {
                assertTrue(sqs.sent.isEmpty(), label);
                assertTrue(sqs.deleted.isEmpty(), label);
                assertEquals(1, errors.size(), label + ": reported, nothing written");
            }
            default -> throw new AssertionError("unknown outcome in " + label + ": " + outcome);
        }
    }
}
