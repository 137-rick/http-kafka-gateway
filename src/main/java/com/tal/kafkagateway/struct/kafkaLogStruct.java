package com.tal.kafkagateway.struct;

public class kafkaLogStruct {

    public kafkaLogStruct(String topic, int partition, Long offset, String Key, String log) {
        this.log = log;
        this.offset = offset;
        this.partition = partition;
        this.topic = topic;
        this.key = Key;
    }

    private String key;

    private String topic;

    private String log;

    private int partition;

    private Long offset;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getLog() {
        return log;
    }

    public void setLog(String log) {
        this.log = log;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }
}
