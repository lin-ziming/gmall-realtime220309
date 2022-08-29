package com.atguigu.realtime.sugar.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author lzc
 * @Date 2022/8/29 11:33
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Traffic {
    private Integer hour;
    private Long pv;
    private Long uv;
    private Long sv;
    
}
