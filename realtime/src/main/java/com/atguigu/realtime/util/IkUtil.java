package com.atguigu.realtime.util;


import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/8/24 14:44
 */
public class IkUtil {
    //TODO
    public static List<String> split(String keyword) {
        ArrayList<String> result = new ArrayList<>();
        // String -> Reader
        // 内存流
        StringReader reader = new StringReader(keyword);
        IKSegmenter seg = new IKSegmenter(reader, true);
    
        Lexeme le = null;
        try {
            le = seg.next();
            while (le != null) {
                String kw = le.getLexemeText();
                result.add(kw);
                le = seg.next();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return result;
    }
    
    public static List<String> split(String keyword, boolean distinct){
        if (distinct) {
            List<String> list = split(keyword);
            HashSet<String> set = new HashSet<>(list);
            return new ArrayList<>(set);
    
        }else{
            return split(keyword);
        }
    }
    
    public static void main(String[] args) {
//        System.out.println(split("我是中国人"));
        System.out.println(split("苹果手机 黑色手机 256g的手机"));
    }
}
