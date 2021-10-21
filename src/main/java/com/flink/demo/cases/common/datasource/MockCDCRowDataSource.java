package com.flink.demo.cases.common.datasource;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 模拟flink CDC数据
 */
public class MockCDCRowDataSource extends RichSourceFunction<Row> {

    private static final Logger logger = LoggerFactory.getLogger(MockCDCRowDataSource.class);

    public static String CLICK_FIELDS = "userId,username,url,clickTime,user_rank,uuid_col,date_col,time_col";

    public static String CLICK_FIELDS_WITH_ROWTIME = "userId,username,url,clickTime.rowtime,user_rank,uuid_col,date_col,time_col";

    public static TypeInformation<Row> USER_CLICK_TYPEINFO = org.apache.flink.api.common.typeinfo.Types.ROW_NAMED(
            new String[]{"userId", "username", "url", "clickTime", "user_rank", "uuid_col", "date_col", "time_col"},
            Types.INT,
            Types.STRING,
            Types.STRING,
            Types.SQL_TIMESTAMP,
            Types.INT,
            Types.STRING,
            Types.STRING,
            Types.STRING
    );

    private boolean running = true;

    private static List<List<String>> clicks = new ArrayList<>();

    static {
        clicks.add(Arrays.asList("I", "65", "userA", "http://127.0.0.1/api/I", "2019-07-23 23:27:29.876", "76", "abcdefg", "20210830", "2021-08-22"));
        clicks.add(Arrays.asList("-U", "65", "userA", "http://127.0.0.1/api/I", "2019-07-23 23:27:29.876", "76", "abcdefg", "20210830", "2021-08-22"));
        clicks.add(Arrays.asList("+U", "65", "userA", "http://127.0.0.1/api/I", "2019-07-23 23:27:30.851", "77", "abcdefg", "20210830", "2021-08-22"));
        clicks.add(Arrays.asList("-U", "65", "userA", "http://127.0.0.1/api/I", "2019-07-23 23:27:30.851", "77", "abcdefg", "20210830", "2021-08-22"));
        clicks.add(Arrays.asList("+U", "65", "userA", "http://127.0.0.1/api/J", "2019-07-23 23:27:31.840", "78", "abcdefg", "20210830", "2021-08-22"));
        clicks.add(Arrays.asList("D", "65", "userA", "http://127.0.0.1/api/I", "2019-07-23 23:27:31.841", "79", "abcdefg", "20210830", "2021-08-20"));
        clicks.add(Arrays.asList("I", "69", "userE", "http://127.0.0.1/api/H", "2019-07-23 23:27:31.893", "80", "abcdefg", "20210830", "2021-08-20"));
        clicks.add(Arrays.asList("I", "67", "userC", "http://127.0.0.1/api/I", "2019-07-23 23:27:32.897", "81", "abcdefg", "20210830", "2021-08-20"));
        clicks.add(Arrays.asList("-U", "69", "userE", "http://127.0.0.1/api/H", "2019-07-23 23:27:31.893", "80", "abcdefg", "20210830", "2021-08-20"));
        clicks.add(Arrays.asList("+U", "69", "userA", "http://127.0.0.1/api/I", "2019-07-23 23:27:25.000", "82", "abcdefg", "20210830", "2021-08-20"));
        clicks.add(Arrays.asList("I", "65", "userA", "http://127.0.0.1/api/I", "2019-07-23 23:27:32.901", "83", "abcdefg", "20210830", "2021-08-25"));
        clicks.add(Arrays.asList("I", "66", "userB", "http://127.0.0.1/api/H", "2019-07-23 23:27:36.908", "84", "abcdefg", "20210830", "2021-08-25"));
        clicks.add(Arrays.asList("-U", "65", "userA", "http://127.0.0.1/api/I", "2019-07-23 23:27:32.901", "83", "abcdefg", "20210830", "2021-08-25"));
        clicks.add(Arrays.asList("+U", "65", "userA", "http://127.0.0.1/api/I", "2019-07-23 23:27:36.915", "85", "abcdefg", "20210830", "2021-08-25"));
        clicks.add(Arrays.asList("-U", "65", "userA", "http://127.0.0.1/api/I", "2019-07-23 23:27:36.915", "85", "abcdefg", "20210830", "2021-08-25"));
        clicks.add(Arrays.asList("+U", "65", "userA", "http://127.0.0.1/api/J", "2019-07-23 23:27:24.019", "86", "abcdefg", "20210830", "2021-08-25"));
        clicks.add(Arrays.asList("-U", "69", "userA", "http://127.0.0.1/api/I", "2019-07-23 23:27:25.000", "82", "abcdefg", "20210830", "2021-08-20"));
        clicks.add(Arrays.asList("+U", "69", "userE", "http://127.0.0.1/api/J", "2019-07-23 23:27:38.916", "87", "abcdefg", "20210830", "2021-08-24"));
        clicks.add(Arrays.asList("-U", "65", "userA", "http://127.0.0.1/api/J", "2019-07-23 23:27:24.019", "86", "abcdefg", "20210830", "2021-08-25"));
        clicks.add(Arrays.asList("+U", "65", "userA", "http://127.0.0.1/api/J", "2019-07-23 23:27:27.189", "88", "abcdefg", "20210830", "2021-08-24"));
        clicks.add(Arrays.asList("D", "66", "userB", "http://127.0.0.1/api/J", "2019-07-23 23:27:48.950", "89", "abcdefg", "20210830", "2021-08-20"));
        clicks.add(Arrays.asList("D", "69", "userE", "http://127.0.0.1/api/K", "2019-07-23 23:27:53.960", "90", "abcdefg", "20210830", "2021-08-20"));
        clicks.add(Arrays.asList("-U", "67", "userC", "http://127.0.0.1/api/I", "2019-07-23 23:27:32.897", "81", "abcdefg", "20210830", "2021-08-20"));
        clicks.add(Arrays.asList("+U", "67", "userC", "http://127.0.0.1/api/J", "2019-07-23 23:28:02.990", "91", "abcdefg", "20210830", "2021-08-24"));
        clicks.add(Arrays.asList("D", "65", "userA", "http://127.0.0.1/api/K", "2019-07-23 23:27:30.960", "92", "abcdefg", "20210830", "2021-08-20"));
        clicks.add(Arrays.asList("I", "69", "userE", "http://127.0.0.1/api/K", "2019-07-23 23:28:08.012", "93", "abcdefg", "20210830", "2021-08-20"));
        clicks.add(Arrays.asList("I", "66", "userB", "http://127.0.0.1/api/K", "2019-07-23 23:28:12.029", "94", "abcdefg", "20210830", "2021-08-24"));
        clicks.add(Arrays.asList("D", "69", "userE", "http://127.0.0.1/api/I", "2019-07-23 23:27:39.918", "95", "abcdefg", "20210830", "2021-08-20"));
        clicks.add(Arrays.asList("-U", "67", "userC", "http://127.0.0.1/api/J", "2019-07-23 23:28:02.990", "91", "abcdefg", "20210830", "2021-08-24"));
        clicks.add(Arrays.asList("+U", "67", "userC", "http://127.0.0.1/api/K", "2019-07-23 23:27:43.931", "96", "abcdefg", "20210830", "2021-08-21"));
        clicks.add(Arrays.asList("-U", "66", "userB", "http://127.0.0.1/api/K", "2019-07-23 23:28:12.029", "94", "abcdefg", "20210830", "2021-08-24"));
        clicks.add(Arrays.asList("+U", "66", "userB", "http://127.0.0.1/api/K", "2019-07-23 23:28:20.000", "97", "abcdefg", "20210830", "2021-08-23"));
        clicks.add(Arrays.asList("-U", "67", "userC", "http://127.0.0.1/api/K", "2019-07-23 23:27:43.931", "96", "abcdefg", "20210830", "2021-08-21"));
        clicks.add(Arrays.asList("+U", "67", "userC", "http://127.0.0.1/api/K", "2019-07-23 23:28:20.001", "98", "abcdefg", "20210830", "2021-08-23"));
        clicks.add(Arrays.asList("D", "67", "userC", "http://127.0.0.1/api/O", "2019-07-23 23:28:20.581", "99", "abcdefg", "20210830", "2021-08-20"));
        clicks.add(Arrays.asList("D", "66", "userB", "http://127.0.0.1/api/K", "2019-07-23 23:28:20.000", "97", "abcdefg", "20210830", "2021-08-23"));
    }

    @Override
    public void run(SourceContext<Row> sourceContext) throws Exception {
        Iterator<List<String>> iterator = clicks.iterator();
        while (iterator.hasNext()) {
            List<String> record = iterator.next();
            Row row = new Row(8);
            String rowKind = record.get(0);
            switch (rowKind) {
                case "I":
                    row.setKind(RowKind.INSERT);
                    break;
                case "-U":
                    row.setKind(RowKind.UPDATE_BEFORE);
                    break;
                case "+U":
                    row.setKind(RowKind.UPDATE_AFTER);
                    break;
                default:
                    row.setKind(RowKind.DELETE);
            }
            row.setField(0, Integer.parseInt(record.get(1)));
            row.setField(1, record.get(2));
            row.setField(2, record.get(3));
            row.setField(3, Timestamp.valueOf(record.get(4)));
            row.setField(4, Integer.parseInt(record.get(5)));
            row.setField(5, record.get(6));
            row.setField(6, record.get(7));
            row.setField(7, record.get(8));
//            logger.info("emit {} -> {}", Timestamp.valueOf(record.get(4)).getTime(), row);
            sourceContext.collect(row);

            Thread.sleep(1000 * 3);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
