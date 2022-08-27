package com.atguigu.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.util.Set;

@Data
@AllArgsConstructor
@Builder
public class TradeProvinceOrderWindow {
    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 省份 ID
    String provinceId;

    // 省份名称
    String provinceName = "";

    // 当天日期
    String curDate;

    // 累计下单次数
    Long orderCount;

    // 累计下单金额
    BigDecimal orderAmount;

    // 时间戳
    Long ts;
    
    Set<String> orderIdSet;
}
