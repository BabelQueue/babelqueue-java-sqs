package com.babelqueue.sqs;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

/**
 * Amazon SQS binding conformance against the vendored canonical suite's {@code sqs}
 * block: the §3 attribute projection and the {@code attempts = ApproximateReceiveCount - 1}
 * reconciliation. No AWS, no network.
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
}
