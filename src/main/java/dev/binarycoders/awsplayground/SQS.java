package dev.binarycoders.awsplayground;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SQS {

    public static void main(String[] args) {
        final var queueName = "SQSTestQueue" + System.currentTimeMillis(); // AWS does not allow create queues with same name in 60 seconds
        final var sqsClient = SqsClient.builder().build();

        System.out.println("Creating queue...");
        final Map<QueueAttributeName, String> attributes = new HashMap<>();
        attributes.put(QueueAttributeName.MESSAGE_RETENTION_PERIOD, "86400");
        attributes.put(QueueAttributeName.DELAY_SECONDS, "1");

        final var createQueueRequest = CreateQueueRequest.builder()
            .queueName(queueName)
            .attributes(attributes)
            .build();
        final var createQueueResponse = sqsClient.createQueue(createQueueRequest);
        final var queueUrl = createQueueResponse.queueUrl();

        System.out.println("Queue URL: " + queueUrl);

        System.out.println("Listing all queues");
        listQueues(sqsClient);

        System.out.println("Sending a message...");
        final SendMessageBatchRequest sendMessageBatchRequest = SendMessageBatchRequest.builder()
            .queueUrl(queueUrl)
            .entries(SendMessageBatchRequestEntry.builder().id("id1").messageBody("I am 1").build(),
                SendMessageBatchRequestEntry.builder().id("id2").messageBody("I am 2").delaySeconds(10).build())
            .build();
        sqsClient.sendMessageBatch(sendMessageBatchRequest);

        System.out.println("Receive a message...");
        final var receiveMessageRequest = ReceiveMessageRequest.builder()
            .queueUrl(queueUrl)
            .maxNumberOfMessages(5)
            .build();
        var receiveMessageResponse = sqsClient.receiveMessage(receiveMessageRequest);

        int counter = 0;
        do {
            try {
                System.out.printf("Receiving message, attempt %d%n", counter);
                receiveMessageResponse = sqsClient.receiveMessage(receiveMessageRequest);
                TimeUnit.SECONDS.sleep(5);
                counter++;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        } while (!receiveMessageResponse.hasMessages() && counter < 5);

        if (receiveMessageResponse.hasMessages()) {
            receiveMessageResponse.messages().forEach(message -> System.out.println("\t" + message.body()));
            receiveMessageResponse.messages().forEach(message -> sqsClient.deleteMessage(builder -> builder
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())));
        } else {
            System.out.println("No messages received.");
        }

        System.out.println("Removing queue...");
        sqsClient.deleteQueue(builder -> builder.queueUrl(queueUrl));
    }

    private static void listQueues(final SqsClient sqsClient) {
        final var listQueuesResponse = sqsClient.listQueues();

        for (final String url : listQueuesResponse.queueUrls()) { // Skips more than pagination. nextToken
            System.out.printf("\t%s%n", url);
        }
    }
}
