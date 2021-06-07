package com.flink.demo.cases.debug;


import org.apache.flink.table.functions.ScalarFunction;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by P0007 on 2019/10/17.
 */
public class FromUnixtime extends ScalarFunction {

    private static final String DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";

    /**
     * 将Long型转成成默认格式的String
     * @param unixtime
     * @return
     */
    public String eval(Long unixtime) {
        return new SimpleDateFormat(DATETIME_PATTERN).format(new Date(unixtime));
    }

    /**
     * 将Long型转成成指定格式的String
     * @param unixtime
     * @return
     */
    public String eval(Long unixtime, String format) {
        return new SimpleDateFormat(format).format(new Date(unixtime));
    }

}
