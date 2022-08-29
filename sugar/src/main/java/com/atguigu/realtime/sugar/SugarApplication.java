package com.atguigu.realtime.sugar;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.realtime.sugar.mapper")
public class SugarApplication {
    
    public static void main(String[] args) {
        SpringApplication.run(SugarApplication.class, args);
    }
    
}
