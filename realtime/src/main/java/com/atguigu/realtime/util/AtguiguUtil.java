package com.atguigu.realtime.util;

import java.text.ParseException;
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
    
    public static <T> List<T> toList(Iterable<T> it) {
        List<T> list = new ArrayList<>();
        //        it.forEach(e -> list.add(e));
        it.forEach(list::add);
        return list;
    }
    
    public static String toDateTime(long ts) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ts);
    }
    
    public static Long toTs(String date) {
        try {
            return new SimpleDateFormat("yyyy-MM-dd").parse(date).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }
    
    // 两个时间, 判断one是否大于two, 如果是则返回true, 否则返回false
    public static boolean isLarger(String one, String two) {
        //2022-08-20 06:23:01.933Z
        // 把z干掉, 然后直接比字符串
        one = one.replaceAll("Z", "");
        two = two.replaceAll("Z", "");
        // compareTo: one 大于tow的时候返回时正的
        return  one.compareTo(two) > 0;
    }
    
    public static void main(String[] args) {
        System.out.println(isLarger("2022-08-20 06:23:01.9Z", "2022-08-20 06:23:01.9Z"));
    }
}
