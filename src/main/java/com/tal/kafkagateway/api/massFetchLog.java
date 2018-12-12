package com.tal.kafkagateway.api;

import com.google.gson.Gson;
import com.tal.kafkagateway.kafka.Consumer;
import com.tal.kafkagateway.kafka.Producer;
import com.tal.kafkagateway.processor.ConsumerLogQueue;
import com.tal.kafkagateway.struct.kafkaLogStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import com.tal.kafkagateway.struct.responseStruct;

import java.util.List;

@RestController
public class massFetchLog {

    Logger log = LoggerFactory.getLogger(massFetchLog.class);

    @Autowired
    ConsumerLogQueue consumerLogQueue;

    @Autowired
    Consumer consumer;

    @Autowired
    Producer producer;

    @RequestMapping(value = "/log/massfetch", method = RequestMethod.GET)
    public String massFetchLogs(Model model, @RequestParam(value = "maxcount", required = true) String maxcount,
                                @RequestParam(value = "topic", required = true) String topic) {
        responseStruct response = new responseStruct();
        Gson gsonHelper = new Gson();

        Integer logMaxLimit;
        try {
            logMaxLimit = Integer.parseInt(maxcount);
        } catch (Exception e) {
            e.printStackTrace();

            response.setCode(231);
            response.setMsg("response maxcount is wrong :" + e.getMessage());
            return gsonHelper.toJson(response);
        }

        if (logMaxLimit <= 0) {

            response.setCode(232);
            response.setMsg("log max limit is wrong ");
            return gsonHelper.toJson(response);
        }

        if (topic.trim().length() == 0) {
            response.setCode(233);
            response.setMsg("topic is empty ");
            return gsonHelper.toJson(response);
        }

        List<kafkaLogStruct> logs = consumerLogQueue.massFetchLog(topic, logMaxLimit);

        response.setCode(0);
        response.setMsg("OK");
        response.setData(logs);

        return gsonHelper.toJson(response);
    }


    @RequestMapping(value = "/log/masspush", method = RequestMethod.POST)
    public String massPush(Model model, @RequestParam(value = "topic", required = true) String topic,
                           @RequestParam(value = "data", required = true) String data) {
        responseStruct response = new responseStruct();
        Gson gsonHelper = new Gson();

        if (topic.length() <= 0) {

            response.setCode(112);
            response.setMsg("topic is wrong");
            return gsonHelper.toJson(response);
        }

        if (data.trim().length() == 0) {
            response.setCode(113);
            response.setMsg("data is empty ");
            return gsonHelper.toJson(response);
        }
        java.util.Random randHelper = new java.util.Random();

        String[] dataArray = data.split("\n");
        for (int dataIndex = 0; dataIndex < dataArray.length; dataIndex++) {
            String[] dataRecord = dataArray[dataIndex].split("\\|\\|\\|");
            if (dataRecord.length > 1 && dataRecord[dataRecord.length - 1].length() > 0) {
                //specify partition
                producer.pushToKafka(topic, dataRecord[dataRecord.length - 1], dataArray[dataIndex]);
            } else {
                //random partition
                producer.pushToKafka(topic, randHelper.nextLong() + "", dataArray[dataIndex]);
            }
        }
        response.setCode(0);
        response.setMsg("OK");

        return gsonHelper.toJson(response);
    }

    /*
    @RequestMapping(value = "/log/getoffset", method = RequestMethod.GET)
    public String getoffset(Model model) {
        //Map<Integer, Long> offset = consumer.getParationOffset();
        //Gson gsonHelper = new Gson();
        //String jsonContent = gsonHelper.toJson(offset);
        //return jsonContent;
    }
*/
    @RequestMapping(value = "/log/close", method = RequestMethod.GET)
    public String closeConsumer(Model model, @RequestParam(value = "pwd", required = true) String pwd) {
        responseStruct response = new responseStruct();
        Gson gsonHelper = new Gson();

        if(pwd.trim().equals("xes123456")){
            consumer.shutdown();
            response.setCode(0);
        }else{
            response.setMsg("密码错误");
            response.setCode(1122);
        }

        return gsonHelper.toJson(response);

    }

    @RequestMapping(value = "/log/queuestat", method = RequestMethod.GET)
    public Integer queueStat(Model model, @RequestParam(value = "topic", required = true) String topic) {
        Integer offset = consumerLogQueue.getQueueLen(topic);
        return offset;
    }

        /*

    @RequestMapping(value = "/log/setoffset", method = RequestMethod.POST)
    public String setOffset(Model model, @RequestParam(value = "offset", required = false) String offset) {
        if (offset == null || offset.trim().length() == 0) {
            return "log max limit is wrong";
        }

        Map<Integer, Long> parationOffset = new HashMap<>();

        return "not support";
        //decode
        JsonReader reader = new JsonReader(new StringReader(offset));
        try {
            reader.beginObject();
            while (reader.hasNext()) {
                Integer itemId = Integer.parseInt(reader.nextName());
                Long itemOffset = Long.parseLong(reader.nextString());
                parationOffset.put(itemId, itemOffset);
            }
            reader.endObject(); // 结束对象的解析
        } catch (Exception e) {
            e.printStackTrace();
            return "wrong offset content format of json";
        }

        if (parationOffset.size() == 0) {
            return "nothing set";
        }

        consumer.setParationOffset(parationOffset);

        return "ok";
    }*/

}
