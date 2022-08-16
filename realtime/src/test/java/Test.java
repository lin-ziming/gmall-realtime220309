/**
 * @Author lzc
 * @Date 2022/8/16 11:04
 */
public class Test {
    public static void main(String[] args) {
        System.out.println("id,coupon_id,range_type,range_id".replaceAll("[^,]+", "$0 varchar"));
    }
}
