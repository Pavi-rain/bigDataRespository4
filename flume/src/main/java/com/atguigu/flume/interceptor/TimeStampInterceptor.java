package com.atguigu.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TimeStampInterceptor implements Interceptor {
//com.atguigu.flume.interceptor.TimeStampInterceptor
    
    private ArrayList<Event> events = new ArrayList<>();
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        //将日志拦下：取出对应header里的可用，取出body里面的对应的日志时间，将ts的值赋给header的key  timestamp

        //1. 获取header头
        Map<String, String> headers = event.getHeaders();

        //2.获取body中的ts
        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);

        JSONObject jsonObject = JSONObject.parseObject(log);

        String ts = jsonObject.getString("ts");

        //3.将ts赋值给 timestamp
        headers.put("timestamp",ts);

        return event;
    }



    @Override
    public List<Event> intercept(List<Event> list) {

        events.clear();
        for (Event event : list) {
            events.add(intercept(event));
        }

        return events;

    }

    @Override
    public void close() {

    }

    public static class Builder implements  Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new TimeStampInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
