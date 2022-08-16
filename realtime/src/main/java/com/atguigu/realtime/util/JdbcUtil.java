package com.atguigu.realtime.util;

import com.atguigu.realtime.common.Constant;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @Author lzc
 * @Date 2022/8/16 10:25
 */
public class JdbcUtil {
    public static Connection getPhoenixConnection() {
        
        String driver = Constant.PHOENIX_DRIVER;
        String url = Constant.PHOENIX_URL;
        
        return getJdbcConnection(driver, url, null, null);
    }
    
    public static Connection getJdbcConnection(String driver, String url, String user, String password){
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("你提供的驱动类错误, 请检查数据库连接器依赖是否导入, 或者驱动名字是否正确: " + driver);
        }
        try {
            return DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new  RuntimeException("你提供的usr或者user或者password 有误, 请检查:url=" + url + ", user=" + user + ",password=" + password);
        }
    
    }
    
    public static void closeConnection(Connection conn) {
        try {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
