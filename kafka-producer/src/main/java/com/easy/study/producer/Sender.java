package com.easy.study.producer;

import org.apache.log4j.MDC;
import org.apache.log4j.NDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.UUID;

public class Sender {

    private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void send(String topic, String message) {

        // the KafkaTemplate provides asynchronous send methods returning a Future
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, message);

        MDC.put("transid", UUID.randomUUID().toString());

        LOGGER.debug("transid get:{}", MDC.get("transid"));
        // you can register a callback with the listener to receive the result of the send asynchronously
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onSuccess(SendResult<String, String> result) {
                LOGGER.info("sent message='{}' with offset={} transid={}", message, result.getRecordMetadata().offset(), NDC.get());
            }

            @Override
            public void onFailure(Throwable ex) {
                LOGGER.error("unable to send message='{}'", message, ex);
            }
        });
    }
}
