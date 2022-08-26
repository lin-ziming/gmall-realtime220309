package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.UserLoginBean;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.AtguiguUtil;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author lzc
 * @Date 2022/8/25 11:03
 */
public class Dws_04_DwsUserUserLoginWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_04_DwsUserUserLoginWindow().init(
            4004,
            2,
            "Dws_04_DwsUserUserLoginWindow",
            Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          DataStreamSource<String> stream) {
        // 1. 先找到用户登录记录
        SingleOutputStreamOperator<JSONObject> loginDataStream = findLoginLog(stream);
        // 2. 找到当日登录用户记录和7日回流用户记录
        SingleOutputStreamOperator<UserLoginBean> beanSteam = findUVAndBack(loginDataStream);
        // 3. 开窗聚合
        SingleOutputStreamOperator<UserLoginBean> resultStream = windowAndAgg(beanSteam);
        // 4. 写出到doris中
        writeToDoris(resultStream);
    }
    
    private SingleOutputStreamOperator<UserLoginBean> windowAndAgg(
        SingleOutputStreamOperator<UserLoginBean> beanSteam) {
        return beanSteam
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<UserLoginBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((bean, ts) -> bean.getTs())
            )
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(
                new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean bean1,
                                                UserLoginBean bean2) throws Exception {
                        bean1.setUuCt(bean1.getUuCt() + bean2.getUuCt());
                        bean1.setBackCt(bean1.getBackCt() + bean2.getBackCt());
                        return bean1;
                    }
                },
                new ProcessAllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void process(Context ctx,
                                        Iterable<UserLoginBean> elements,
                                        Collector<UserLoginBean> out) throws Exception {
    
                        UserLoginBean bean = elements.iterator().next();
                        
                        bean.setStt(AtguiguUtil.toDateTime(ctx.window().getStart()));
                        bean.setEdt(AtguiguUtil.toDateTime(ctx.window().getEnd()));
                        
                        bean.setCurDate(AtguiguUtil.toDate(System.currentTimeMillis()));
                        
                        out.collect(bean);
    
                    }
                }
            );
    }
    
    private SingleOutputStreamOperator<UserLoginBean> findUVAndBack(
        SingleOutputStreamOperator<JSONObject> loginDataStream) {
        return loginDataStream
            .keyBy(obj -> obj.getJSONObject("common").getString("uid"))
            .process(new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {
                
                private ValueState<String> lastLoginDateState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    lastLoginDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastLoginDateState", String.class));
                }
                
                @Override
                public void processElement(JSONObject obj,
                                           Context ctx,
                                           Collector<UserLoginBean> out) throws Exception {
                    Long ts = obj.getLong("ts");
                    String thisLoginDate = AtguiguUtil.toDate(ts);
                    // 最后一次登录日期
                    String lastLoginDate = lastLoginDateState.value();
                    
                    Long uuCt = 0L;
                    Long backCt = 0L;
                    // 如果这次登录日期与最后一次登录日期不相等, 则这次是今天的第一次登录
                    if (!thisLoginDate.equals(lastLoginDate)) {
                        // 今天第一次登录
                        lastLoginDateState.update(thisLoginDate);  // 把状态更新为这次登录到日期
                        uuCt = 1L;
                        
                        // 判断这个用户是否维基回流用户
                        // 当最后一次登录日期不会空, 表示这个用户不是新登录用户,才有必要判断是否为回流
                        if (lastLoginDate != null) {
                            Long thisLoginTs = AtguiguUtil.toTs(thisLoginDate);
                            Long lastLoginTs = AtguiguUtil.toTs(lastLoginDate);
                            
                            if ((thisLoginTs - lastLoginTs) / 1000 / 60 / 60 / 24 > 7) {
                                backCt = 1L;
                            }
                        }
                    }
                    
                    if (uuCt == 1) {
                        out.collect(new UserLoginBean("", "",
                                                      "",
                                                      backCt, uuCt,
                                                      ts
                        ));
                        
                    }
                }
            });
    }
    
    private SingleOutputStreamOperator<JSONObject> findLoginLog(DataStreamSource<String> stream) {
        /*
            1. 主动登录
                用户主动打开登录页面, 输入用户名和密码, 实现登录
                
                用户在访问其他页面的过程中, 当执行一些需要登录才能的操作的时候, 页面会跳转到登录页面, 然后完成登录
                
                  当主动登录成功之后, 会今天另外一个页面, 这个页面的 last_page_id is login  并且当前页面 uid is not null
            
          
            2. 自动登录
                一旦进入用户页面, 当前网页根据你的一些cookie或缓存等等, 在后台自动失效登录
                    找到这个用户当天第一条登录成功的记录.
                    什么记录才算?
                        第一个包含uid字段的日志
                            uid is not null
        
        
         */
        return stream
            .map(JSON::parseObject)
            .filter(obj -> {
                String uid = obj.getJSONObject("common").getString("uid");
                String lastPageId = obj.getJSONObject("page").getString("last_page_id");
                
                // 自动登录: uid != null && lastPageId = null
                
                // 主动登录(登录成功之后的页面): uid != null && (lastPageId='login' )
                
                return uid != null && (lastPageId == null || "login".equals(lastPageId));
            });
        
        
    }
    
    private void writeToDoris(SingleOutputStreamOperator<UserLoginBean> resultStream) {
        resultStream
            .map(bean -> {
                SerializeConfig config = new SerializeConfig();
                config.propertyNamingStrategy = PropertyNamingStrategy.SnakeCase;  // 转成json的时候, 属性名使用下划线
                return JSON.toJSONString(bean, config);
            })
            .addSink(FlinkSinkUtil.getDorisSink("gmall2022.dws_user_user_login_window"));
    }
    
    
}
/*
DwsUserUserLoginWindow

 */