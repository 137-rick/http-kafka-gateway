package com.tal.kafkagateway.struct;

public class kafkaLogStruct {

    public kafkaLogStruct(String topic, int partition, Long offset, String log) {
        this.log = log;
        this.offset = offset;
        this.partition = partition;
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    private String topic;

    private String log;

    private int partition;

    private Long offset;

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
