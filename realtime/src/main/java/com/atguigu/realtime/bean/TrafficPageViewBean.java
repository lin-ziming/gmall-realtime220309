package com.atguigu.realtime.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TrafficPageViewBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // app 版本号
    String vc;
    // 渠道
    String ch;
    // 地区
    String ar;
    // 新老访客状态标记
//    @JSONField(name = "is_new")  // 一个一个的设置, 比较麻烦
    String isNew ;
    // 当天日期
    String curDate;
    // 独立访客数
    Long uvCt;
    // 会话数
    Long svCt;
    // 页面浏览数
    Long pvCt;
    // 累计访问时长
    Long durSum;
    // 跳出会话数
    Long ujCt;
    // 时间戳
    @JSONField(serialize = false) // 这个字段不需要序列化到json字符串中, 可以加这个注解
    Long ts;
}
