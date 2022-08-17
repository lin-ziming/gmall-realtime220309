package com.atguigu.realtime.util;

import java.text.SimpleDateFormat;

/**
 * @Author lzc
 * @Date 2022/8/17 10:39
 */
public class AtguiguUtil {
    public static String toDate(Long ts) {
        return new SimpleDateFormat("yyyy-MM-dd").format(ts);
    }
}
