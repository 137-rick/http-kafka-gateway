package com.tal.kafkagateway.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerCallback implements Callback {

    private Logger log = LoggerFactory.getLogger(ProducerCallback.class);

    public void onCompletion(RecordMetadata record, Exception exception) {
        if (exception != null) {
            log.error(exception.getMessage() + "\r\n" +
                    "topic:" + record.topic() + "\r\n" +
                    record.toString());
            exception.printStackTrace();
        }
    }
}
