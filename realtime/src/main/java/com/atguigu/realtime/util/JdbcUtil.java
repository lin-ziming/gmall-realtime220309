package com.atguigu.realtime.util;

import com.atguigu.realtime.common.Constant;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

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
    
    public static Connection getJdbcConnection(String driver, String url, String user, String password) {
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("你提供的驱动类错误, 请检查数据库连接器依赖是否导入, 或者驱动名字是否正确: " + driver);
        }
        try {
            return DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("你提供的url或者user或者password 有误, 请检查:url=" + url + ", user=" + user + ",password=" + password);
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
    
    public static <T> List<T> queryList(Connection conn, String querySql, Object[] args, Class<T> tClass) {
        
        ArrayList<T> result = new ArrayList<>();
        
        try {
            PreparedStatement ps = conn.prepareStatement(querySql);
            // 给sql中的占位符赋值
            for (int i = 0; args != null && i < args.length; i++) {
                ps.setObject(i +1, args[i]);
            }
    
            ResultSet resultSet = ps.executeQuery();
    
            ResultSetMetaData metaData = resultSet.getMetaData(); // 元数据: 列名 列的类型 列的别名
            int columnCount = metaData.getColumnCount();  // 查询的结果有多少列
    
    
            // 遍历出结果集中的每一行
            while (resultSet.next()) {
                // 每一行数据, 封装到一个T类型的对象中, 然后放入到result这个集合中
                T t = tClass.newInstance(); // 利用反射创建一个T类型的对象
                
                // 给T中属性赋值, 属性的值从resultSet获取
                // 从resultSet里面查看有多少列, 每一列在T中应该对应一个属性
                for (int i = 1; i <= columnCount; i++) { // 列的索引应该从1开始,
                    // 列名
                    String columnName = metaData.getColumnLabel(i);
                    Object v = resultSet.getObject(i);
    
                    BeanUtils.setProperty(t, columnName, v);
                }
                
                result.add(t);
    
            }
    
    
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        
        return result;
    }
    
    public static void main(String[] args) {
    
        /*List<JSONObject> list = queryList(getPhoenixConnection(),
                                                 "select * from dim_spu_info",
                                                 null,
                                                 JSONObject.class
        );
    
        for (JSONObject  obj: list) {
            System.out.println(obj);
            
        }*/
    
    
        List<TM> list = queryList(getJdbcConnection("com.mysql.jdbc.Driver","jdbc:mysql://hadoop162:3306/gmall2022?useSSL=false","root", "aaaaaa"),
                                          "select * from base_trademark",
                                          null,
                                          TM.class
        );
    
        for (TM  obj: list) {
            System.out.println(obj);
        
        }
    }
    
    @NoArgsConstructor
    @AllArgsConstructor
    @Data
    public static class TM{
        private String tm_name;
        private String logo_url;
        private int id;
    }
}
