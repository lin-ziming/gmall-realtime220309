package com.atguigu.realtime.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@Builder
public class TradeSkuOrderBean {
    // 窗口起始时间
//    @Builder.Default
    String stt = "2022";
    // 窗口结束时间
    String edt;
    // 品牌 ID
    String trademarkId;
    // 品牌名称
    String trademarkName;
    // 一级品类 ID
    String category1Id;
    // 一级品类名称
    String category1Name;
    // 二级品类 ID
    String category2Id;
    // 二级品类名称
    String category2Name;
    // 三级品类 ID
    String category3Id;
    // 三级品类名称
    String category3Name;
    // sku_id
    String skuId;
    // sku 名称
    String skuName;
    // spu_id
    String spuId;
    // spu 名称
    String spuName;
    // 当天日期
    String curDate;
    // 原始金额
    // 活动减免金额
    
    @JSONField(name = "order_origin_total_amount")
    BigDecimal originalAmount;
    @JSONField(name = "order_activity_reduce_amount")
    BigDecimal activityAmount;
    // 优惠券减免金额
    @JSONField(name = "order_coupon_reduce_amount")
    BigDecimal couponAmount;
    // 下单金额
    BigDecimal orderAmount;
    // 时间戳
    Long ts;
    
    public static void main(String[] args) {
        TradeSkuOrderBean bean = TradeSkuOrderBean.builder()
            .skuId("a")
            .ts(123L)
            .orderAmount(new BigDecimal(10))
            .build();
        System.out.println(bean);
    }
}
