package com.atguigu.realtime.app.dwd.log;

import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.common.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/8/17 9:35
 */
public class Dwd_01_DwdBaseLogApp extends BaseAppV1 {
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
    
        stream.print();
    }
}
