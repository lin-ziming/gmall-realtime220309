package com.atguigu.realtime.sugar.service;

import com.atguigu.realtime.sugar.bean.Spu;
import com.atguigu.realtime.sugar.bean.Tm;
import com.atguigu.realtime.sugar.bean.Traffic;
import com.atguigu.realtime.sugar.mapper.TradeMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2022/8/29 10:27
 */
@Service
public class TradeServiceImpl implements TradeService {
    
    @Autowired // TradeMapper 对象, sprintboot会自动创建并赋值
    TradeMapper tradeMapper;
    @Override
    public BigDecimal gmv(int date) {
        return tradeMapper.gmv(date);
    }
    
    @Override
    public List<Spu> gmvBySpu(int date) {
        return tradeMapper.gmvBySpu(date);
    }
    
    @Override
    public List<Tm> gmvByTm(int date) {
        return tradeMapper.gmvByTm(date);
    }
    
    @Override
    public List<Traffic> statsTraffic(int date) {
        return tradeMapper.statsTraffic(date);
    }
    
    @Override
    public List<Map<String, Object>> kw(int date) {
        return tradeMapper.kw(date);
    }
    
}
