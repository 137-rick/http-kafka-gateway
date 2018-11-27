package com.tal.kafkagateway.kafka;

import com.tal.kafkagateway.config.ConfigHelper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Component
@Scope("singleton")
public class Producer {

    @Autowired
    private ConfigHelper configHelper;

    private KafkaProducer<String, String> kafkaProducer;


    public void pushToKafka(String Topic, String Key, String Data) {
        kafkaProducer = KafkaUtil.getProducer(configHelper.getKafkaserver(), configHelper.getSecurityprotocol(), configHelper.getSaslmechanism(),configHelper.getUser(),configHelper.getPasswd());

        ProducerRecord<String, String> record = new ProducerRecord<>(Topic, Key, Data);
        kafkaProducer.send(record, new ProducerCallback());
    }

    @PreDestroy
    public void shutdown() {
        try {
            if(kafkaProducer != null){
                kafkaProducer.flush();
                kafkaProducer.close();
                kafkaProducer = null;
                KafkaUtil.cleanProducer();
            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
