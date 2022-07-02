package com.pcm.azure.servicebus.controller;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
@Service

/**
 *  This class is using the examples found here.
 *  https://docs.microsoft.com/en-us/samples/azure/azure-sdk-for-java/servicebus-samples/
 */
public class SendController {

    @Value("${servicebus.connection-string}")
    private String connectionString;

    @Value("${servicebus.queue.name}")
    private String queueName;

    @GetMapping("/send")
    public String send() {
        try {
            // create a Service Bus Sender client for the queue
            ServiceBusSenderClient senderClient = new ServiceBusClientBuilder()
                    .connectionString(connectionString)
                    .sender()
                    .queueName(queueName)
                    .buildClient();

            // send one message to the queue
            senderClient.sendMessage(new ServiceBusMessage("Hello, World!"));
            log.info("Sent a single message to the queue: " + queueName);
        } catch (Exception e) {
            log.error("Failed to send the message.", e);
        }
        return "message sent.";
    }
}