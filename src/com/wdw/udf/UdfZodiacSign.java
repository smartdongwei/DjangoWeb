package com.wdw.udf;

/**
 * 本代码为udf的练习
 * 函数输入的时一个日期，输出的时该日期表示的用户星座的字符串
 */

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.SimpleDateFormat;
import java.util.Date;

@Description(name = "zodiacSign",
            value = "zodiacSign(date) -输入date string "+
                    "或者月份和天数 最后返回对应的星座",
        extended = "例如：\n"
                + "> select zodiacSign(date_string) from demo;\n"
                + "> select zodiacSign(month ,day) from demo;\n"
              )
public class UdfZodiacSign extends UDF {
    private SimpleDateFormat df;

    public UdfZodiacSign(){
        df = new SimpleDateFormat("MM-dd-yyyy");
    }

    public String evaluate(Date bday){
        return this.evaluate(bday.getMonth() , bday.getDay());
    }

    public String evaluate(String bday){
        Date date = null;
        try{
            date = df.parse(bday);
        }catch (Exception e){
            return null;
        }
        return this.evaluate(date.getMonth()+1,date.getDay());
    }

    public String evaluate(Integer month,Integer day){
        if(month.equals(1)){
            if(day <20){
                return "Caparicorn";
            }else{
                return "Aquarius";
            }
        }else{
            return "other";
        }
    }


}
