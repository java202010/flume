package com.atguigu.hive;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @auther ai
 * @date 2020/7/28 18:47
 */
public class Lower extends UDF {
    public String evaluate(String s){
        if (s==null){
            return null;
        }
        return s.toLowerCase();
    }
}
