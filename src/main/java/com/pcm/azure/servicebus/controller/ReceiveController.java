package com.pcm.azure.servicebus.controller;

import com.azure.messaging.servicebus.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.Disposable;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@RestController
@Slf4j
@Service

/**
 * This class is using examples found here.
 * https://docs.microsoft.com/en-us/samples/azure/azure-sdk-for-java/servicebus-samples/
 *
 */
public class ReceiveController {

    @Value("${servicebus.connection-string}")
    private String connectionString;

    @Value("${servicebus.queue.name}")
    private String queueName;

    @GetMapping("/receive")
    public String receive() throws InterruptedException {

        AtomicBoolean sampleSuccessful = new AtomicBoolean(true);
        CountDownLatch countdownLatch = new CountDownLatch(1);

        ServiceBusReceiverAsyncClient receiver = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .receiver()
                .disableAutoComplete()
                .queueName(queueName)
                .buildAsyncClient();

        Disposable subscription = receiver.receiveMessages()
                .flatMap(message -> {
                    boolean messageProcessed = processMessage(message);
                    if (messageProcessed) {
                        return receiver.complete(message);
                    } else {
                        return receiver.abandon(message);
                    }
                }).subscribe(
                        (ignore) -> log.info("Message processed."),
                        error -> sampleSuccessful.set(false)
                );

        // Subscribe is not a blocking call so we wait here so the program does not end.
        countdownLatch.await(10, TimeUnit.SECONDS);

        // Disposing of the subscription will cancel the receive() operation.
        subscription.dispose();

        // Close the receiver.
        receiver.close();

        return "Message received.";

    }

    private static boolean processMessage(ServiceBusReceivedMessage message) {
        log.info("Sequence #: {}. Contents: {}", message.getSequenceNumber(), message.getBody());
        return true;
    }


}
