package com.atguigu.realtime.function;

import com.atguigu.realtime.util.IkUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @Author lzc
 * @Date 2022/8/24 14:38
 */
@FunctionHint(output = @DataTypeHint("row<kw string>"))
public class IkAnalyzer extends TableFunction<Row> {
    
    public void eval(String keyword){
        //keyword 使用ik分词器进行分词, 分出来几个词, 就是几行
       List<String> kws = IkUtil.split(keyword);
       
        for (String kw : kws) {
            collect(Row.of(kw));
            
        }
    }

}
