# http-kafka-gateway
an gateway for kafka by http

 * 一个服务网关,使用Java实现
 * 可以使用http获取消费kafka内数据
 * 可以使用http批量发送数据给kafka
 * 支持 kafka 0.9+ 协议
 * 使用JAAS验证 SASL\_PLAN

 * 后续有空会持续维护

# Protocol

 * consumer get http://127.0.0.1:9099/dk/log/massfetch?maxcount=1000&topic=topicname


 * producer post http://127.0.0.1:9099/dk/log/masspush topic=xxx&data=urlencode(xxxx) x-www-form-urlencoded
 
 
 
 # Start Server
 * nohup java -XX:-MaxFDLimit -Xms3750m -Xmx3750m -XX:ReservedCodeCacheSize=240m -XX:+UseCompressedOops -jar kafkagateway-0.0.1-SNAPSHOT.jar &
 
 
 # Option for startup
<pre>
<code>
# you can change setting by startup with -- prefix
# eg: --kg.user=jaas_accountxxxx --kg.kafkatopic=xxx,xxxx
#logging
logging.path=./logs/
logging.file=./logs/kafkagateway.log
logging.level.com.tal.kafkagateway=INFO 

# temp queue backup
kg.queuedumppath=./dump/ 

# kafka
kg.kafkatopic=topic_a,topic_b
kg.kafkagroupid=kafkagateway_group
kg.kafkaserver=kafkabrokerip:kafkabrokerport,kafkabrokerip:kafkabrokerport
kg.user=jaas_account
kg.passwd=jaas_pwd 
kg.securityprotocol=SASL_PLAINTEXT
kg.saslmechanism=PLAIN
</code>
</pre>