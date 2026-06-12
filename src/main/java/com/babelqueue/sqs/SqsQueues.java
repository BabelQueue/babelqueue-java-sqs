package com.babelqueue.sqs;

/** Internal helpers for SQS queue URLs. */
final class SqsQueues {

    private SqsQueues() {}

    /** The queue name = the last non-empty path segment of an SQS queue URL. */
    static String nameFromUrl(String queueUrl) {
        if (queueUrl == null) {
            return "default";
        }
        String[] parts = queueUrl.split("/");
        for (int i = parts.length - 1; i >= 0; i--) {
            if (!parts[i].isEmpty()) {
                return parts[i];
            }
        }
        return "default";
    }
}
