package com.tal.kafkagateway.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.Properties;

public class KafkaUtil {

    private static KafkaConsumer<String, String> kafkaConsumerHandle;

    private static KafkaProducer<String, String> kafkaProducerHandle;


    public static KafkaConsumer<String, String> getConsumer(String serverList, String groupID, String Protocol, String Mechanism, String User, String Passwd) {
        if (kafkaConsumerHandle == null) {

            Properties props = new Properties();

            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverList);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

            if (Protocol.length() > 0) {
                props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, Protocol);
            }
            if (Mechanism.length() > 0) {
                props.put(SaslConfigs.SASL_MECHANISM, Mechanism);
            }
            if (User.length() > 0 || Passwd.length() > 0) {
                props.put("sasl.jaas.config",
                        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + User + "\" password=\"" + Passwd + "\";");
            }

            kafkaConsumerHandle = new KafkaConsumer<>(props);
        }

        return kafkaConsumerHandle;
    }

    public static KafkaProducer<String, String> getProducer(String serverList, String Protocol, String Mechanism, String User, String Passwd) {

        if (kafkaProducerHandle == null) {

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverList);
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
            //props.put("unclean.leader.election.enable", false); // test
            //props.put("min.insync.replicas", 1); //test
            //props.put("block.on.buffer.full", true); //test

            props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
            props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
            props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

            if (Protocol.length() > 0) {
                props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, Protocol);
            }
            if (Mechanism.length() > 0) {
                props.put(SaslConfigs.SASL_MECHANISM, Mechanism);
            }
            if (User.length() > 0 || Passwd.length() > 0) {
                props.put("sasl.jaas.config",
                        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + User + "\" password=\"" + Passwd + "\";");
            }

            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            kafkaProducerHandle = new KafkaProducer<>(props);
        }

        return kafkaProducerHandle;

    }

    public static void cleanConsumer() {
        kafkaConsumerHandle = null;
    }

    public static void cleanProducer() {
        kafkaProducerHandle = null;
    }
}
