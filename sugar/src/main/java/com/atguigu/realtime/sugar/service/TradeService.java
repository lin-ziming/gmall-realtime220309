package com.atguigu.realtime.sugar.service;

import com.atguigu.realtime.sugar.bean.Spu;

import java.math.BigDecimal;
import java.util.List;

public interface TradeService {
    BigDecimal gmv(int date);
    
    
    List<Spu> gmvBySpu(int date);
}
