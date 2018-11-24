package com.tal.kafkagateway.processor;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.tal.kafkagateway.config.ConfigHelper;
import com.tal.kafkagateway.struct.kafkaLogStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Component
@Scope("singleton")
public class ConsumerLogQueue {

    private int maxQueueLength = 20000;

    //private LinkedBlockingQueue<kafkaLogStruct> LogQueue = new LinkedBlockingQueue<>(this.maxQueueLength);

    private HashMap<String, LinkedBlockingQueue<kafkaLogStruct>> LogQueueList = new HashMap<>();

    @Autowired
    private ConfigHelper configHelper;

    //log obj
    private Logger log = LoggerFactory.getLogger(ConsumerLogQueue.class);

    public void initQueue(String Topic) {

        if (Topic.trim().isEmpty()) {
            return;
        }

        //init queue
        LogQueueList.put(Topic, new LinkedBlockingQueue<>(this.maxQueueLength));

        //load queue dump
        String dumpFilePath = configHelper.getQueuedumppath() + Topic.trim() + ".dump";

        Gson gsonHelper = new Gson();

        log.info("start load the dump queue file:" + dumpFilePath);
        try {
            File file = new File(dumpFilePath);
            if (file.isFile() && file.exists()) {
                InputStreamReader read = new InputStreamReader(new FileInputStream(file), "UTF-8");
                BufferedReader bufferedReader = new BufferedReader(read);
                String fileLine = null;
                int dataCount = 0;

                while ((fileLine = bufferedReader.readLine()) != null) {
                    //ignore wrong format
                    try {
                        kafkaLogStruct kafkaData = gsonHelper.fromJson(fileLine.trim(), kafkaLogStruct.class);
                        if (kafkaData.getLog().length() > 0) {
                            dataCount++;
                            LogQueueList.get(Topic).put(kafkaData);
                        }
                    } catch (JsonSyntaxException e) {
                        e.printStackTrace();
                        continue;
                    }
                }
                log.info("loaded the dump queue file:" + dumpFilePath + " count:" + dataCount);
                read.close();
            } else {
                log.info("load queue dump file fail:" + dumpFilePath);
            }
        } catch (Exception e) {
            log.info("load queue dump file exception:" + dumpFilePath);
            e.printStackTrace();
        }
    }


    public Integer getQueueLen(String Topic) {
        if (LogQueueList.containsKey(Topic)) {
            return LogQueueList.get(Topic).size();
        }
        return -1;
    }

    public boolean checkAvalible(String Topic) {
        if (LogQueueList.containsKey(Topic) && LogQueueList.get(Topic).size() >= this.maxQueueLength / 2) {
            return false;
        }
        return true;
    }


    public void insertDataQueue(String Topic, kafkaLogStruct data) {
        if (data != null && LogQueueList.containsKey(Topic)) {
            try {
                LogQueueList.get(Topic).put(data);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public List<kafkaLogStruct> massFetchLog(String Topic, Integer fetchMaxSize) {
        List<kafkaLogStruct> result = new LinkedList<>();

        if (!LogQueueList.containsKey(Topic)) {
            return result;
        }

        for (int fetchCount = 0; fetchCount < fetchMaxSize; fetchCount++) {
            kafkaLogStruct log;
            log = null;

            try {
                log = LogQueueList.get(Topic).poll(10L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (log == null) {
                break;
            }

            result.add(log);
        }
        return result;
    }


    @PreDestroy
    public void destroy() {

        //dump the topic queue
        for (Map.Entry<String, LinkedBlockingQueue<kafkaLogStruct>> queueList : LogQueueList.entrySet()) {

            String dumpFilePath = configHelper.getQueuedumppath() + queueList.getKey().trim() + ".dump";

            Gson gsonHelper = new Gson();

            BufferedWriter bw = null;
            log.info("start dump the queue file:" + dumpFilePath);

            try {
                File file = new File(dumpFilePath);

                //clean up old
                //file.deleteOnExit();

                //create empty file
                file.createNewFile();

                FileWriter fw = new FileWriter(file.getAbsoluteFile());
                bw = new BufferedWriter(fw);

                kafkaLogStruct queueData = null;

                int dataCount = 0;

                //dump the data
                while ((queueData = queueList.getValue().poll(1L, TimeUnit.MILLISECONDS)) != null) {
                    dataCount++;
                    bw.write(gsonHelper.toJson(queueData) + "\r\n");
                }
                log.info("dump queue file:" + dumpFilePath + " count:" + dataCount);

                bw.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
}
