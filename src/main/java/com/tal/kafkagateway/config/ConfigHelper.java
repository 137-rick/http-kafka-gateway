package com.tal.kafkagateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "kg")
public class ConfigHelper {

    private String kafkaserver;

    private String kafkagroupid;

    private String kafkatopic;

    private String user;

    private String passwd;

    private String queuedumppath;

    private String securityprotocol;

    private String saslmechanism;

    public String getSecurityprotocol() {
        return securityprotocol;
    }

    public void setSecurityprotocol(String securityprotocol) {
        this.securityprotocol = securityprotocol;
    }

    public String getSaslmechanism() {
        return saslmechanism;
    }

    public void setSaslmechanism(String saslmechanism) {
        this.saslmechanism = saslmechanism;
    }

    public String getQueuedumppath() {
        return queuedumppath;
    }

    public void setQueuedumppath(String queuedumppath) {
        this.queuedumppath = queuedumppath;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPasswd() {
        return passwd;
    }

    public void setPasswd(String passwd) {
        this.passwd = passwd;
    }

    public String getKafkaserver() {
        return kafkaserver;
    }

    public void setKafkaserver(String kafkaserver) {
        this.kafkaserver = kafkaserver;
    }

    public String getKafkagroupid() {
        return kafkagroupid;
    }

    public void setKafkagroupid(String kafkagroupid) {
        this.kafkagroupid = kafkagroupid;
    }

    public String getKafkatopic() {
        return kafkatopic;
    }

    public void setKafkatopic(String kafkatopic) {
        this.kafkatopic = kafkatopic;
    }
}