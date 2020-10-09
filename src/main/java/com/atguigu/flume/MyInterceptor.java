package com.atguigu.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

class MyIntercepter implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        if ((body[0]<'z'&&body[0]>'a')||(body[0]<'Z'&&body[0]>'A')){
            event.getHeaders().put("head","type1");
        }else if (body[0]>'0'&&body[0]<'9'){
            event.getHeaders().put("head","type2");
        }

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() {

    }
    public static class MyBuilder implements Builder{

        @Override
        public Interceptor build() {
            return new MyIntercepter();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
