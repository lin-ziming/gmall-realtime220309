import com.alibaba.fastjson.JSONObject;

import java.util.Arrays;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/8/16 14:20
 */
public class Test2 {
    public static void main(String[] args) {
        JSONObject data = new JSONObject();
        data.put("a", 10);
        data.put("b", 10);
        data.put("c", 10);
        List<String> columns = Arrays.asList("a,c".split(","));
        
        
        /*while (it.hasNext()) {
            String key = it.next();
            if (!columns.contains(key)) {
                it.remove();
            }
        }*/
    
    
        data.keySet().removeIf(key -> !columns.contains(key));
        
   
    
        /*for (String key : data.keySet()) {
            if (!columns.contains(key)) {
                data.remove(key);
            }
        }*/
        System.out.println(data);
    }
}
