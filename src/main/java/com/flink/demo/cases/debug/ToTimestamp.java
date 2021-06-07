package com.flink.demo.cases.debug;


import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by P0007 on 2019/10/17.
 */
public class ToTimestamp extends ScalarFunction {

    /**
     * 将Long值转换成Timestamp, 默认乘以1000
     * @param unixtime
     * @return
     */
    public Timestamp eval(Long unixtime) {
        return new Timestamp(unixtime);
    }

    /**
     * 将String值转换成Timestamp，需要指定date format
     * @param unixtime
     * @param format
     * @return 返回该unixtime对应的Timestamp类型的值。转换失败返回0
     */
    public Timestamp eval(String unixtime, String format) {
        SimpleDateFormat formater = new SimpleDateFormat(format);
        try {
            Date date = formater.parse(unixtime);
            long dateTime = date.getTime();
            return new Timestamp(dateTime);
        } catch (ParseException e) {
            return new Timestamp(0L);
        }
    }

}
