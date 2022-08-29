package com.atguigu.realtime.sugar.mapper;

import com.atguigu.realtime.sugar.bean.Spu;
import com.atguigu.realtime.sugar.bean.Tm;
import com.atguigu.realtime.sugar.bean.Traffic;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

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
    
    /*
    | Apple iPhone 12                                                            |     24946378 |
    | TCL巨幕私人影院电视 4K超高清 AI智慧屏  液晶平板电视机                      |     17001368 |
    | 小米10                                                                     |     11986114 |
    | HUAWEI P40                                                                 |  10424553.52 |
    | 华为智慧屏 4K全面屏智能电视机                                              |      5370501 |
     */
    @Select("select " +
        "spu_name, " +
        "sum(order_amount) order_amount " +
        "from dws_trade_sku_order_window partition(par#{date}) " +
        "group by spu_name")
    List<Spu> gmvBySpu(int date);
    
    @Select("select trademark_name, sum(order_amount) order_amount from dws_trade_sku_order_window partition(par#{date}) group by trademark_name")
    List<Tm> gmvByTm(int date);
    
    @Select("select hour(stt) hour, sum(pv_ct) pv, sum(uv_ct) uv, sum(sv_ct) sv    " +
        "from dws_traffic_vc_ch_ar_is_new_page_view_window partition(par#{date}) " +
        "group by hour(stt);")
    List<Traffic> statsTraffic(int date);
}

/*
 */
