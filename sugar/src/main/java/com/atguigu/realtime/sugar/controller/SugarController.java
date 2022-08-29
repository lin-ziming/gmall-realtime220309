package com.atguigu.realtime.sugar.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.sugar.bean.Spu;
import com.atguigu.realtime.sugar.bean.Tm;
import com.atguigu.realtime.sugar.bean.Traffic;
import com.atguigu.realtime.sugar.service.TradeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2022/8/29 10:30
 */

@RestController
public class SugarController {
    
    @Autowired
    TradeService tradeService;
    
    @RequestMapping("/sugar/gmv")
    public String gmv(int date) {
        
        BigDecimal gmv = tradeService.gmv(date);
        
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
        
        result.put("data", gmv);
        
        return result.toJSONString();
    }
    
    
    @RequestMapping("/sugar/gmv/spu")
    public String gmvBySpu(int date) {
        
        List<Spu> list = tradeService.gmvBySpu(date);
        
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
        
        JSONObject data = new JSONObject();
        JSONArray categories = new JSONArray();
        for (Spu spu : list) {
            categories.add(spu.getSpu_name());
        }
        data.put("categories", categories);
        
        JSONArray series = new JSONArray();
        
        JSONObject obj = new JSONObject();
        
        obj.put("name", "spu名字");
        JSONArray data1 = new JSONArray();
        for (Spu spu : list) {
            data1.add(spu.getOrder_amount());
        }
        obj.put("data", data1);
        
        series.add(obj);
        
        data.put("series", series);
        
        result.put("data", data);
        
        return result.toJSONString();
    }
    
    
    @RequestMapping("/sugar/gmv/tm")
    public String gmvByTm(int date) {
        
        List<Tm> list = tradeService.gmvByTm(date);
        
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
        
        JSONArray data = new JSONArray();
        
        for (Tm tm : list) {
            JSONObject obj = new JSONObject();
            
            obj.put("name", tm.getTrademark_name());
            obj.put("value", tm.getOrder_amount());
            
            data.add(obj);
        }
        
        
        result.put("data", data);
        
        return result.toJSONString();
    }
    
    
    @RequestMapping("/sugar/traffic")
    public String traffic(int date) {
        
        List<Traffic> list = tradeService.statsTraffic(date);
        
        // key:hour value;Traffic
        Map<Integer, Traffic> hourAndTrafficMap = new HashMap<>();
        for (Traffic traffic : list) {
            hourAndTrafficMap.put(traffic.getHour(), traffic);
        }
        
        
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
        
        JSONObject data = new JSONObject();
        
        JSONArray categories = new JSONArray();
        for (int i = 0; i < 24; i++) {
            categories.add(i);
        }
        data.put("categories", categories);
        
        JSONArray series = new JSONArray();
        
        JSONObject pv = new JSONObject();
        pv.put("name", "pv");
        JSONArray pvData = new JSONArray();
        pv.put("data", pvData);
        series.add(pv);
        
        JSONObject uv = new JSONObject();
        uv.put("name", "uv");
        JSONArray uvData = new JSONArray();
        uv.put("data", uvData);
        series.add(uv);
        
        JSONObject sv = new JSONObject();
        sv.put("name", "sv");
        JSONArray svData = new JSONArray();
        sv.put("data", svData);
        series.add(sv);
        
        data.put("series", series);
        
        
        for (int hour = 0; hour < 24; hour++) {
            // 根据key'获取value, 如果key不存在, 则返回默认值
            Traffic traffic = hourAndTrafficMap.getOrDefault(hour, new Traffic(hour, 0L, 0L, 0L));
            pvData.add(traffic.getPv());
            uvData.add(traffic.getUv());
            svData.add(traffic.getSv());
        }
        result.put("data", data);
        
        return result.toJSONString();
    }
    
    
    @RequestMapping("/sugar/kw")
    public String kw(int date) {
        
        List<Map<String, Object>> list = tradeService.kw(date);
        
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
        
        JSONArray data = new JSONArray();
        
        for (Map<String, Object> map : list) {
            
            JSONObject obj = new JSONObject();
            obj.put("name", map.get("keyword"));
            obj.put("value", map.get("score"));
            
            data.add(obj);
            
        }
        
        
        result.put("data", data);
        
        return result.toJSONString();
    }
}
