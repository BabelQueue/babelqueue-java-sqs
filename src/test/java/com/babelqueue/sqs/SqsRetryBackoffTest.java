package com.babelqueue.sqs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.babelqueue.EnvelopeCodec;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** ADR-0034 §9 / A9: {@code retry_backoff*} params on the SQS consumer; {@code releaseDelaySeconds} is the base alias. */
class SqsRetryBackoffTest {

    private static final String URL = "https://sqs.eu-central-1.amazonaws.com/123456789012/orders";
    private static final String URN = "urn:babel:orders:created";

    /** Fail one message received {@code receiveCount} times and return the visibility timeout it was released with. */
    private static int releasedWith(SqsConsumer.Builder configure, FakeSqsClient sqs, int receiveCount) {
        sqs.seed(URL, EnvelopeCodec.encode(EnvelopeCodec.make(URN, Map.of("x", 1), "orders", null)), receiveCount);
        configure.handler(URN, (e, m) -> { throw new IllegalStateException("boom"); }).build().poll();
        assertEquals(1, sqs.visibilityChanges.size());
        return sqs.visibilityChanges.get(0).visibilityTimeout();
    }

    @Test
    void defaultsKeepConstantReleaseDelay() {
        for (int receives = 1; receives <= 4; receives++) {
            FakeSqsClient sqs = new FakeSqsClient();
            assertEquals(30, releasedWith(SqsConsumer.builder(sqs, URL).releaseDelaySeconds(30), sqs, receives));
        }
    }

    @Test
    void exponentialScheduleIsCappedByMax() {
        int[] expected = {5, 10, 20, 30, 30};
        for (int receives = 1; receives <= expected.length; receives++) {
            FakeSqsClient sqs = new FakeSqsClient();
            SqsConsumer.Builder b = SqsConsumer.builder(sqs, URL)
                .retryBackoff(5).retryBackoffMultiplier(2).retryBackoffMax(30);
            assertEquals(expected[receives - 1], releasedWith(b, sqs, receives), "receive " + receives);
        }
    }

    @Test
    void releaseDelaySecondsIsAliasForTheBase() {
        FakeSqsClient a = new FakeSqsClient();
        FakeSqsClient b = new FakeSqsClient();
        int viaAlias = releasedWith(SqsConsumer.builder(a, URL).releaseDelaySeconds(4).retryBackoffMultiplier(3), a, 3);
        int viaParam = releasedWith(SqsConsumer.builder(b, URL).retryBackoff(4).retryBackoffMultiplier(3), b, 3);
        assertEquals(36, viaAlias);
        assertEquals(viaAlias, viaParam);
    }

    @Test
    void fullJitterDrawsBelowTheCeiling() {
        FakeSqsClient sqs = new FakeSqsClient();
        SqsConsumer.Builder b = SqsConsumer.builder(sqs, URL)
            .retryBackoff(10).retryJitter("full").randomSource(() -> 0.5);
        assertEquals(5, releasedWith(b, sqs, 1));
    }

    @Test
    void zeroBaseStaysZeroEvenWhenThePowerOverflows() {
        FakeSqsClient sqs = new FakeSqsClient();
        SqsConsumer.Builder b = SqsConsumer.builder(sqs, URL).retryBackoff(0).retryBackoffMultiplier(2);
        assertEquals(0, releasedWith(b, sqs, 1100));
    }

    @Test
    void jitterNameIsNormalisedLikeTheCore() {
        FakeSqsClient sqs = new FakeSqsClient();
        SqsConsumer.Builder b = SqsConsumer.builder(sqs, URL)
            .retryBackoff(10).retryJitter(" FULL ").randomSource(() -> 0.5);
        assertEquals(5, releasedWith(b, sqs, 1));
        FakeSqsClient none = new FakeSqsClient();
        assertEquals(10, releasedWith(SqsConsumer.builder(none, URL).retryBackoff(10).retryJitter("None"), none, 1));
    }

    @Test
    void resultIsClampedToTheSqsRange() {
        FakeSqsClient sqs = new FakeSqsClient();
        SqsConsumer.Builder b = SqsConsumer.builder(sqs, URL).retryBackoff(1000).retryBackoffMultiplier(100);
        assertEquals(43_200, releasedWith(b, sqs, 6));
    }

    @Test
    void invalidParamsAreRejected() {
        SqsConsumer.Builder b = SqsConsumer.builder(new FakeSqsClient(), URL);
        assertThrows(IllegalArgumentException.class, () -> b.retryBackoff(-1));
        assertThrows(IllegalArgumentException.class, () -> b.retryBackoffMultiplier(0.5));
        assertThrows(IllegalArgumentException.class, () -> b.retryBackoffMax(-1));
        assertThrows(IllegalArgumentException.class, () -> b.retryJitter("equal"));
    }
}
