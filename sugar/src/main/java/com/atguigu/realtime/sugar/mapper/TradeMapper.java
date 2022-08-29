package com.atguigu.realtime.sugar.mapper;

import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;

/**
 * @Author lzc
 * @Date 2022/8/29 10:21
 */
public interface TradeMapper {
    
    @Select("select " +
        "sum(order_amount) " +
        "order_amount " +
        "from dws_trade_sku_order_window " +
        "partition(par#{date});")
    BigDecimal gmv(int date);
}
