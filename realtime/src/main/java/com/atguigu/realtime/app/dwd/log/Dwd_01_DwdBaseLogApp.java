package com.atguigu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.AtguiguUtil;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2022/8/17 9:35
 */
public class Dwd_01_DwdBaseLogApp extends BaseAppV1 {
    
    private final String PAGE = "page";
    private final String ERR = "err";
    private final String ACTION = "action";
    private final String DISPLAY = "display";
    private final String START = "start";
    
    
    public static void main(String[] args) {
        new Dwd_01_DwdBaseLogApp().init(
            3001,
            2,
            "Dwd_01_DwdBaseLogApp",
            Constant.TOPIC_ODS_LOG
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          DataStreamSource<String> stream) {
        
        // 1. etl
        SingleOutputStreamOperator<JSONObject> etledStreram = etl(stream);
        // 2. 纠正新老客户标签
        SingleOutputStreamOperator<JSONObject> validatedStream = validateNewOrOld(etledStreram);
        // 3. 分流
        Map<String, DataStream<JSONObject>> streams = splitStream(validatedStream);
        
        // 4. 写出到kafka中
        writeToKafka(streams);
    }
    
    private void writeToKafka(Map<String, DataStream<JSONObject>> streams) {
        streams.get(PAGE).map(JSONAware::toJSONString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        streams.get(ERR).map(JSONAware::toJSONString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        streams.get(ACTION).map(JSONAware::toJSONString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
        streams.get(START).map(JSONAware::toJSONString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        streams.get(DISPLAY).map(JSONAware::toJSONString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
    }
    
    private Map<String, DataStream<JSONObject>> splitStream(SingleOutputStreamOperator<JSONObject> validatedStream) {
        OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("display") {};
        OutputTag<JSONObject> actionTag = new OutputTag<JSONObject>("action") {};
        OutputTag<JSONObject> pageTag = new OutputTag<JSONObject>("page") {};
        OutputTag<JSONObject> errTag = new OutputTag<JSONObject>("err") {};
        // 5个流:
        // 启动日志: 主流
        // 页面 曝光 活动 错误  测输出流
        SingleOutputStreamOperator<JSONObject> startStream = validatedStream
            .process(new ProcessFunction<JSONObject, JSONObject>() {
                @Override
                public void processElement(JSONObject obj,
                                           Context ctx,
                                           Collector<JSONObject> out) throws Exception {
                    if (obj.containsKey("start")) {
                        // 启动日志
                        out.collect(obj);
                    } else {
                        JSONObject common = obj.getJSONObject("common");
                        JSONObject page = obj.getJSONObject("page");
                        Long ts = obj.getLong("ts");
                        
                        
                        // 1. 曝光
                        JSONArray displays = obj.getJSONArray("displays");
                        if (displays != null) {
                            for (int i = 0; i < displays.size(); i++) {
                                JSONObject display = displays.getJSONObject(i);
                                display.putAll(common); // 把common中的字段复制到display中
                                display.putAll(page);
                                display.put("ts", ts);
                                
                                ctx.output(displayTag, display);
                            }
                            // 处理完displays之后, 应该把这个字段移除
                            obj.remove("displays");
                        }
                        
                        
                        // 2. 活动
                        JSONArray actions = obj.getJSONArray("actions");
                        if (actions != null) {
                            for (int i = 0; i < actions.size(); i++) {
                                JSONObject action = actions.getJSONObject(i);
                                action.putAll(common); // 把common中的字段复制到display中
                                action.putAll(page);
                                ctx.output(actionTag, action);
                                
                            }
                            obj.remove("actions");
                        }
                        
                        // 3. 错误
                        if (obj.containsKey("err")) {
                            ctx.output(errTag, obj);
                            obj.remove("err");
                        }
                        
                        // 4. page
                        if (page != null) {
                            ctx.output(pageTag, obj);
                        }
                        
                    }
                    
                }
            });
    
        Map<String, DataStream<JSONObject>>  result = new HashMap<>();
        result.put(START, startStream);
        result.put(DISPLAY, startStream.getSideOutput(displayTag));
        result.put(ACTION, startStream.getSideOutput(actionTag));
        result.put(ERR, startStream.getSideOutput(errTag));
        result.put(PAGE, startStream.getSideOutput(pageTag));
    
    
        return result;
    
    
    }
    
    private SingleOutputStreamOperator<JSONObject> validateNewOrOld(
        SingleOutputStreamOperator<JSONObject> etledStreram) {
        return etledStreram
            .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
            .map(new RichMapFunction<JSONObject, JSONObject>() {
                
                private ValueState<String> firstVisitDateState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("firstVisitDate", String.class));
                }
                
                @Override
                public JSONObject map(JSONObject value) throws Exception {
                    
                    JSONObject common = value.getJSONObject("common");
                    
                    String firstVisit = firstVisitDateState.value();
                    Long ts = value.getLong("ts");
                    String today = AtguiguUtil.toDate(ts);
                    
                    // is_new=1有可能有误, 需要修改
                    if ("1".equals(common.getString("is_new"))) {
                        // 状态是空, 确实是第一次, 不用修改, 但是需要更新状态
                        if (firstVisit == null) {
                            firstVisitDateState.update(today);
                        } else if (!today.equals(firstVisit)) { // 状态不为空, 是否和今天相等, 如果不相等, 需要更新
                            common.put("is_new", "0");
                        }
                        
                    } else {
                        // is_new是0, 状态是空
                        // 意味着, 用户曾经访问过, 但是程序中没有记录第一次访问时间
                        // 把状态更新成昨天
                        if (firstVisitDateState == null) {
                            String yesterday = AtguiguUtil.toDate(ts - (24 * 60 * 60 * 1000));
                            firstVisitDateState.update(yesterday);
                        }
                    }
                    return value;
                }
            });
    }
    
    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream
            .filter(json -> {
                try {
                    JSON.parseObject(json);
                } catch (Exception e) {
                    System.out.println("json格式有误, 请检查: " + json);
                    return false;
                }
                return true;
            })
            .map(JSON::parseObject);
    }
}
/*
1. 维度表数据
     java -jar gmall2020-mock-db-2021...
       只会产生 user_info
       
     bootstrap...
     
2. yarn集群跑flink
        per-job  过时
        application 用这个
        session
            先起动集群
            在提交job

3. ....

-------

修复的逻辑:
     定义一个状态: 存储年月日, 用户第一次访问的年月日
  
  is_new = 1
     state和今天是同一天或者状态中没有值
       不用修复
       
     
     state和今天不是同一天
        is_new = 0
  is_new = 0
      一定是老用户, 不用修复
      更新状态: 更新成昨天

 */