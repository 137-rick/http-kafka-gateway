package com.tal.kafkagateway.kafka;

import com.tal.kafkagateway.config.ConfigHelper;
import com.tal.kafkagateway.processor.ConsumerLogQueue;
import com.tal.kafkagateway.struct.kafkaLogStruct;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;

@Component
@Scope("singleton")
public class Consumer implements DisposableBean, Runnable {

    @Autowired
    private ConfigHelper configHelper;

    @Autowired
    ConsumerLogQueue consumerLogQueue;

    private Thread thread;

    private Logger log = LoggerFactory.getLogger(Consumer.class);

    //private Map<Integer, Long> parationOffset = new HashMap<>();

    private boolean isDestroy = false;

    private boolean startWithOffset = false;

    private KafkaConsumer<String, String> consumer;

    private List<String> topicList = new ArrayList<>();

    //is pause
    private HashMap<String, Boolean> isPause = new HashMap<String, Boolean>();

    @PostConstruct
    public void start() {
        this.startConsumer(false);
    }

    @Override
    public void run() {
        try {

            //when the queue is full pause the queue

            String[] topicListStringArray = configHelper.getKafkatopic().split(",");

            topicList.clear();

            //init all list
            for (int topicIndex = 0; topicIndex < topicListStringArray.length; topicIndex++) {

                //init queue
                consumerLogQueue.initQueue(topicListStringArray[topicIndex].trim());

                //init pause status
                isPause.put(topicListStringArray[topicIndex], false);

                //init topic list
                topicList.add(topicListStringArray[topicIndex]);
            }

            //consumer start
            consumer = KafkaUtil.getConsumer(configHelper.getKafkaserver(), configHelper.getKafkagroupid(), configHelper.getSecurityprotocol(),configHelper.getSaslmechanism(),
                    configHelper.getUser(), configHelper.getPasswd());

            //start with set offset
            if (this.startWithOffset) {
                /*
                for (Map.Entry<Integer, Long> offsetItem : this.parationOffset.entrySet()) {
                    TopicPartition topicPartition = new TopicPartition(configHelper.getKafkatopic(), offsetItem.getKey());
                    consumer.assign(Arrays.asList(topicPartition));
                    consumer.seek(topicPartition, offsetItem.getValue());
                }
                this.startWithOffset = false;*/
            } else {
                consumer.subscribe(topicList);
            }


            while (!this.isDestroy) {

                ConsumerRecords<String, String> records = consumer.poll(1000);

                for (ConsumerRecord<String, String> record : records) {

                    if (record.value().trim().length() == 0) {
                        continue;
                    }

                    kafkaLogStruct content = new kafkaLogStruct( record.topic(), record.partition(), record.offset(), record.key(), record.value());

                    consumerLogQueue.insertDataQueue(content.getTopic(), content);
                }


                //pause && resume
                for (String topic : topicList) {

                    List<PartitionInfo> parationList = consumer.partitionsFor(topic);
                    //not avalible pause poll
                    if (!consumerLogQueue.checkAvalible(topic) && !isPause.get(topic)) {
                        List<TopicPartition> pList = new ArrayList<>();

                        for (PartitionInfo partitionInfo : parationList) {
                            int partitionid = partitionInfo.partition();
                            TopicPartition partition = new TopicPartition(topic, partitionid);
                            pList.add(partition);
                        }
                        consumer.pause(pList);

                        isPause.put(topic, true);
                    }

                    //avalible on queue resume poll
                    if (consumerLogQueue.checkAvalible(topic) && isPause.get(topic)) {
                        List<TopicPartition> pList = new ArrayList<>();

                        for (PartitionInfo partitionInfo : parationList) {
                            int partitionid = partitionInfo.partition();
                            TopicPartition partition = new TopicPartition(topic, partitionid);
                            pList.add(partition);
                        }
                        consumer.resume(pList);
                        isPause.put(topic, false);
                    }

                }

            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!this.isDestroy) {
                throw e;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
            this.consumer = null;
            KafkaUtil.cleanConsumer();
        }
    }

    public boolean startConsumer(boolean startWithOffset) {
        if (this.thread == null || !this.thread.isAlive()) {
            log.info("start kafka Log Consumer...");

            this.isDestroy = false;
            this.startWithOffset = startWithOffset;
            this.thread = new Thread(this);
            this.thread.start();
            return true;
        }

        log.error("kafka consumer thread is alive...");
        return false;
    }

    /*

    public void setParationOffset(Map<Integer, Long> paratitionOffset) {
        this.parationOffset = paratitionOffset;
        this.shutdown();
        consumerLogQueue.cleanQueue();
        this.startConsumer(true);
    }
*/
 /*
    public Map<Integer, Long> getParationOffset() {
        return this.parationOffset;
    }
*/

    /**
     * stop consumer
     */
    public void shutdown() {
        log.info("shutdown the consumer threat");
        this.isDestroy = true;
        consumer.wakeup();
        try {
            this.thread.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @PreDestroy
    public void destroy() {
        this.shutdown();
    }
}
