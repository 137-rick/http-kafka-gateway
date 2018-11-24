package com.tal.kafkagateway.struct;

import java.util.List;

public class responseStruct {

    private int code;

    private String msg;

    private List<kafkaLogStruct> data ;

    public responseStruct() {
        this.code=0;
        this.msg="Ok";
        this.data = null;
    }

    public responseStruct(int code,String Msg, List<kafkaLogStruct> data ){
        this.code = code;
        this.msg = Msg;
        this.data = data;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public List<kafkaLogStruct> getData() {
        return data;
    }

    public void setData(List<kafkaLogStruct> data) {
        this.data = data;
    }
}
