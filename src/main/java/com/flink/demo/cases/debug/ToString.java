package com.flink.demo.cases.debug;


import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Created by P0007 on 2020/4/9.
 */
public class ToString extends ScalarFunction {

    /**
     * 将timestamp转成给定format格式的string
     * @param timestamp
     * @param format
     * @return
     */
    public String eval(Timestamp timestamp, String format) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
        LocalDateTime localDateTime = timestamp.toLocalDateTime();
        ZonedDateTime zonedDateTime = localDateTime.atZone(ZoneId.systemDefault());
        String dateStr = zonedDateTime.format(formatter);
        return dateStr;
    }

}
