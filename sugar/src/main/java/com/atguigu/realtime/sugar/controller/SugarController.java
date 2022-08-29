package com.atguigu.realtime.sugar.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.sugar.service.TradeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;

/**
 * @Author lzc
 * @Date 2022/8/29 10:30
 */

@RestController
public class SugarController{

    @Autowired
    TradeService tradeService;
    @RequestMapping("/sugar/gmv")
    public String gmv(int date){
    
        BigDecimal gmv = tradeService.gmv(date);
    
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
    
        result.put("data", gmv);
    
        return result.toJSONString();
    }
    
}
