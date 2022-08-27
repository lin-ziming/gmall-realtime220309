package com.atguigu.realtime.util;

import com.alibaba.fastjson.JSONObject;

import java.sql.Connection;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/8/27 8:53
 */
public class DimUtil {
    
    public static JSONObject readDimFromPhoenix(Connection phoenixConn, String table, String id) {
        String sql = "select * from " + table + " where id=?";
        
        String[] args = {id};
        
        // 一个通用的jdbc查询方法, 返回值有可能多行, 所以返回List集合
        List<JSONObject> list = JdbcUtil.queryList(phoenixConn, sql, args, JSONObject.class);
    
        // 对当前这个sql语句, 一定只有一行, 直接get(0)
        return list.get(0);
    }
}
