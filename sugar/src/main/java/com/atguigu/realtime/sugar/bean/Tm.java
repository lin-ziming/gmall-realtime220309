package com.atguigu.realtime.sugar.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @Author lzc
 * @Date 2022/8/29 11:33
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Tm {
    private String trademark_name;
    private BigDecimal order_amount;
    
}
