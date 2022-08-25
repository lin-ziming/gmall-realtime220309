package com.atguigu.realtime.util;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/8/17 10:39
 */
public class AtguiguUtil {
    public static String toDate(Long ts) {
        return new SimpleDateFormat("yyyy-MM-dd").format(ts);
    }
    
    public static <T>List<T> toList(Iterable<T> it) {
        List<T> list = new ArrayList<>();
//        it.forEach(e -> list.add(e));
        it.forEach(list::add);
        return list;
    }
    
    public static String toDateTime(long ts) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ts);
    }
}
