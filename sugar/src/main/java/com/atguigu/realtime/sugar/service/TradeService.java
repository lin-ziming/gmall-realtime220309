package com.atguigu.realtime.sugar.service;

import com.atguigu.realtime.sugar.bean.Spu;
import com.atguigu.realtime.sugar.bean.Tm;
import com.atguigu.realtime.sugar.bean.Traffic;

import java.math.BigDecimal;
import java.util.List;

public interface TradeService {
    BigDecimal gmv(int date);
    
    
    List<Spu> gmvBySpu(int date);
    
    
    List<Tm> gmvByTm(int date);
    
    List<Traffic> statsTraffic(int date);
}
