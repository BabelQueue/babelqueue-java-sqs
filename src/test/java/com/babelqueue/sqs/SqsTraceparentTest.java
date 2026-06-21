package com.babelqueue.sqs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * The §3 SQS wiring of the out-of-band {@code traceparent} header (ADR-0028): produce
 * merges it beside the contract {@code bq-*} attributes (contract wins, 10-attribute cap),
 * consume surfaces the delivered attributes as a {@code Map<String,String>}, and a
 * publish→consume round-trip carries the header end to end so the consumer span is a true
 * child of the producer span. No AWS, no network (a fake {@link SqsClient} preserves
 * attributes on the round-trip).
 */
class SqsTraceparentTest {

    private static final String URL = "https://sqs.eu-central-1.amazonaws.com/123456789012/orders";
    private static final String TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    @Test
    void publishWithHeadersMergesTraceparentBesideContractAttributes() {
        FakeSqsClient sqs = new FakeSqsClient();
        Envelope env = EnvelopeCodec.make("urn:babel:orders:created", Map.of("x", 1), "orders", "trace-1");

        SqsPublisher.create(sqs, URL).publishWithHeaders(env, Map.of("traceparent", TRACEPARENT));

        Map<String, MessageAttributeValue> a = sqs.sent.get(0).messageAttributes();
        // out-of-band traceparent rides as a String attribute beside the contract bq-*
        assertEquals(TRACEPARENT, a.get("traceparent").stringValue());
        assertEquals("String", a.get("traceparent").dataType());
        // contract projection is intact
        assertEquals("urn:babel:orders:created", a.get("bq-job").stringValue());
        assertEquals("trace-1", a.get("bq-trace-id").stringValue());
    }

    @Test
    void headerlessPublishWithHeadersIsByteIdenticalToPlainPublish() {
        FakeSqsClient withHeaders = new FakeSqsClient();
        FakeSqsClient plain = new FakeSqsClient();
        Envelope env = EnvelopeCodec.make("urn:babel:orders:created", Map.of("x", 1), "orders", "trace-1");

        SqsPublisher.create(withHeaders, URL).publishWithHeaders(env, Map.of());
        SqsPublisher.create(plain, URL).publishWithHeaders(env, null);

        Map<String, MessageAttributeValue> empty = withHeaders.sent.get(0).messageAttributes();
        Map<String, MessageAttributeValue> nullMap = plain.sent.get(0).messageAttributes();
        // same attribute set as the plain contract projection — no extra keys
        assertEquals(SqsAttributes.project(env).keySet(), empty.keySet());
        assertEquals(SqsAttributes.project(env).keySet(), nullMap.keySet());
        assertFalse(empty.containsKey("traceparent"));
    }

    @Test
    void contractAttributeWinsACollision() {
        Envelope env = EnvelopeCodec.make("urn:babel:orders:created", Map.of("x", 1), "orders", "trace-1");
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("bq-job", "SPOOFED");      // collides with a contract key -> must not win
        headers.put("traceparent", TRACEPARENT);

        Map<String, MessageAttributeValue> a = SqsAttributes.projectWithHeaders(env, headers);

        assertEquals("urn:babel:orders:created", a.get("bq-job").stringValue()); // contract wins
        assertEquals(TRACEPARENT, a.get("traceparent").stringValue());          // header still carried
    }

    @Test
    void capIsRespectedAndContractAttributesAreNeverCrowdedOut() {
        Envelope env = EnvelopeCodec.make("urn:babel:orders:created", Map.of("x", 1), "orders", "trace-1");
        Map<String, String> headers = new LinkedHashMap<>();
        // 6 contract attrs already; many out-of-band headers must not exceed the 10 cap.
        for (int i = 0; i < 12; i++) {
            headers.put("h" + i, "v" + i);
        }

        Map<String, MessageAttributeValue> a = SqsAttributes.projectWithHeaders(env, headers);

        assertEquals(SqsAttributes.MAX_ATTRIBUTES, a.size());          // exactly capped
        assertEquals("urn:babel:orders:created", a.get("bq-job").stringValue()); // contract intact
        assertEquals("trace-1", a.get("bq-trace-id").stringValue());
    }

    @Test
    void extractSurfacesDeliveredAttributesAsAMap() {
        FakeSqsClient sqs = new FakeSqsClient();
        Envelope env = EnvelopeCodec.make("urn:babel:orders:created", Map.of("x", 1), "orders", "trace-1");
        SqsPublisher.create(sqs, URL).publishWithHeaders(env, Map.of("traceparent", TRACEPARENT));

        Map<String, String>[] seen = new Map[]{null};
        SqsConsumer.builder(sqs, URL)
            .handler("urn:babel:orders:created", (e, message) -> seen[0] = SqsHeaders.of(message))
            .build()
            .poll();

        assertEquals(TRACEPARENT, seen[0].get("traceparent"));
        assertEquals("trace-1", seen[0].get("bq-trace-id")); // contract attrs surface too
    }

    @Test
    void extractOnAMessageWithNoAttributesIsEmpty() {
        FakeSqsClient sqs = new FakeSqsClient();
        sqs.seed(URL, EnvelopeCodec.encode(
            EnvelopeCodec.make("urn:babel:orders:created", Map.of("x", 1), "orders", null)), 1);

        Map<String, String>[] seen = new Map[]{Map.of("sentinel", "x")};
        SqsConsumer.builder(sqs, URL)
            .handler("urn:babel:orders:created", (e, message) -> seen[0] = SqsHeaders.of(message))
            .build()
            .poll();

        assertTrue(seen[0].isEmpty());
        assertTrue(SqsHeaders.of(null).isEmpty());
    }

    @Test
    void endToEndTraceparentMakesConsumerSpanAChildOfTheProducerSpan() throws Exception {
        InMemorySpanExporter exporter = InMemorySpanExporter.create();
        SdkTracerProvider provider = SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(exporter))
            .build();
        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder().setTracerProvider(provider).build();
        Tracer tracer = sdk.getTracer("test");

        FakeSqsClient sqs = new FakeSqsClient();
        SqsPublisher publisher = SqsPublisher.create(sqs, URL);

        // Producer: a PRODUCER span injects its traceparent onto the SQS attributes via the
        // otel HeaderSender seam -> SqsPublisher.publishWithHeaders.
        com.babelqueue.otel.Tracing.publish(
            tracer, "urn:babel:orders:created", Map.of("order_id", 7), "orders",
            (envelope, headers) -> publisher.publishWithHeaders(envelope, headers));

        // Consumer: wrapHandler reads the delivered attributes and parents the CONSUMER span
        // on the carried traceparent.
        SqsConsumer.builder(sqs, URL)
            .handler("urn:babel:orders:created", (env, message) ->
                com.babelqueue.otel.Tracing.wrapHandler(tracer, e -> { }, () -> SqsHeaders.of(message))
                    .handle(env))
            .build()
            .poll();

        List<SpanData> spans = exporter.getFinishedSpanItems();
        SpanData producer = spanByName(spans, "publish urn:babel:orders:created");
        SpanData consumer = spanByName(spans, "process urn:babel:orders:created");
        assertEquals(producer.getSpanContext().getSpanId(), consumer.getParentSpanContext().getSpanId());
        assertEquals(producer.getSpanContext().getTraceId(), consumer.getTraceId());
        assertTrue(consumer.getParentSpanContext().isRemote());
        provider.close();
    }

    @Test
    void traceparentRidesOutOfBandNotInsideTheEnvelope() {
        FakeSqsClient sqs = new FakeSqsClient();
        Envelope env = EnvelopeCodec.make("urn:babel:orders:created", Map.of("x", 1), "orders", "trace-1");
        SqsPublisher.create(sqs, URL).publishWithHeaders(env, Map.of("traceparent", TRACEPARENT));

        SendMessageRequest req = sqs.sent.get(0);
        assertFalse(req.messageBody().contains("traceparent"));     // GR-1: not in the body
        assertEquals("trace-1", EnvelopeCodec.decode(req.messageBody()).traceId()); // GR-4: trace_id preserved
        assertNull(EnvelopeCodec.decode(req.messageBody()).deadLetter());
    }

    private static SpanData spanByName(List<SpanData> spans, String name) {
        return spans.stream().filter(s -> s.getName().equals(name)).findFirst()
            .orElseThrow(() -> new AssertionError("span not found: " + name));
    }
}
