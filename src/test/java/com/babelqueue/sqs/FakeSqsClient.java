package com.babelqueue.sqs;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

/** An in-memory {@link SqsClient} for unit tests — no AWS, no network. */
final class FakeSqsClient implements SqsClient {

    final List<SendMessageRequest> sent = new ArrayList<>();
    final List<String> deleted = new ArrayList<>();
    ReceiveMessageRequest lastReceive;

    private final Map<String, Deque<Message>> queues = new HashMap<>();
    private final RuntimeException error;
    private int counter;

    FakeSqsClient() {
        this(null);
    }

    FakeSqsClient(RuntimeException error) {
        this.error = error;
    }

    @Override
    public String serviceName() {
        return "sqs";
    }

    @Override
    public void close() {
        // no-op
    }

    @Override
    public SendMessageResponse sendMessage(SendMessageRequest request) {
        if (error != null) {
            throw error;
        }
        sent.add(request);
        counter++;
        String handle = "rh-" + counter;
        push(request.queueUrl(), Message.builder()
            .body(request.messageBody())
            .messageAttributes(request.messageAttributes())
            .receiptHandle(handle)
            .attributesWithStrings(Map.of("ApproximateReceiveCount", "1"))
            .build());
        return SendMessageResponse.builder().messageId(handle).build();
    }

    @Override
    public ReceiveMessageResponse receiveMessage(ReceiveMessageRequest request) {
        lastReceive = request;
        if (error != null) {
            throw error;
        }
        Deque<Message> queue = queues.getOrDefault(request.queueUrl(), new ArrayDeque<>());
        int max = request.maxNumberOfMessages() == null ? 10 : request.maxNumberOfMessages();
        List<Message> taken = new ArrayList<>();
        while (!queue.isEmpty() && taken.size() < max) {
            taken.add(queue.pollFirst());
        }
        return ReceiveMessageResponse.builder().messages(taken).build();
    }

    @Override
    public DeleteMessageResponse deleteMessage(DeleteMessageRequest request) {
        if (error != null) {
            throw error;
        }
        deleted.add(request.receiptHandle());
        return DeleteMessageResponse.builder().build();
    }

    /** Seed a raw message with a chosen ApproximateReceiveCount. */
    void seed(String queueUrl, String body, int receiveCount) {
        seed(queueUrl, body, String.valueOf(receiveCount));
    }

    /** Seed a raw message with a raw ApproximateReceiveCount string (or none when null). */
    void seed(String queueUrl, String body, String receiveCount) {
        counter++;
        Message.Builder message = Message.builder().body(body).receiptHandle("seed-" + counter);
        if (receiveCount != null) {
            message.attributesWithStrings(Map.of("ApproximateReceiveCount", receiveCount));
        }
        push(queueUrl, message.build());
    }

    private void push(String queueUrl, Message message) {
        queues.computeIfAbsent(queueUrl, key -> new ArrayDeque<>()).addLast(message);
    }
}
