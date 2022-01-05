package com.flink.demo.cases.common.datasource;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 乱序数据源
 * 按顺序写出数据，时间错乱，未指定时间戳和水印
 */
public class OutOfOrderRowDataSource extends RichSourceFunction<Row> {

    private static final Logger logger = LoggerFactory.getLogger(OutOfOrderRowDataSource.class);

    public static String CLICK_FIELDS = "userId,username,url,clickTime,rowtime.rowtime";

    public static TypeInformation CLICK_TYPEINFO = Types.ROW(
            new String[]{"userId", "username", "url", "clickTime"},
            new TypeInformation[]{
                    Types.INT(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.SQL_TIMESTAMP()
            });

    private boolean running = true;

    private static List<List<String>> clicks = new ArrayList<>();

    static {
        clicks.add(Arrays.asList("65", "用户A", "http://127.0.0.1/api/I", "2019-07-23 23:27:29.876", "88", "abcdefg", "20210830", "2021-08-22"));
        clicks.add(Arrays.asList("65", "用户A", "http://127.0.0.1/api/I", "2019-07-23 23:27:30.851", "88", "abcdefg", "20210830", "2021-08-22"));
        clicks.add(Arrays.asList("65", "用户A", "http://127.0.0.1/api/J", "2019-07-23 23:27:31.840", "88", "abcdefg", "20210830", "2021-08-22"));
        clicks.add(Arrays.asList("65", "用户A", "http://127.0.0.1/api/I", "2019-07-23 23:27:31.841", "88", "abcdefg", "20210830", "2021-08-20"));
        clicks.add(Arrays.asList("69", "用户E", "http://127.0.0.1/api/H", "2019-07-23 23:27:31.893", "88", "abcdefg", "20210830", "2021-08-20"));
        clicks.add(Arrays.asList("67", "用户C", "http://127.0.0.1/api/I", "2019-07-23 23:27:32.897", "88", "abcdefg", "20210830", "2021-08-20"));
        clicks.add(Arrays.asList("69", "用户A", "http://127.0.0.1/api/I", "2019-07-23 23:27:25.000", "88", "abcdefg", "20210830", "2021-08-20"));//晚到数据
        clicks.add(Arrays.asList("65", "用户A", "http://127.0.0.1/api/I", "2019-07-23 23:27:32.901", "88", "abcdefg", "20210830", "2021-08-25"));
        clicks.add(Arrays.asList("66", "用户B", "http://127.0.0.1/api/H", "2019-07-23 23:27:36.908", "88", "abcdefg", "20210830", "2021-08-25"));
        clicks.add(Arrays.asList("65", "用户A", "http://127.0.0.1/api/I", "2019-07-23 23:27:36.915", "88", "abcdefg", "20210830", "2021-08-25"));
        clicks.add(Arrays.asList("65", "用户A", "http://127.0.0.1/api/J", "2019-07-23 23:27:24.019", "88", "abcdefg", "20210830", "2021-08-25"));//晚到数据
        clicks.add(Arrays.asList("69", "用户E", "http://127.0.0.1/api/J", "2019-07-23 23:27:38.916", "88", "abcdefg", "20210830", "2021-08-24"));
        clicks.add(Arrays.asList("65", "用户A", "http://127.0.0.1/api/J", "2019-07-23 23:27:27.189", "88", "abcdefg", "20210830", "2021-08-24"));//晚到数据
        clicks.add(Arrays.asList("66", "用户B", "http://127.0.0.1/api/J", "2019-07-23 23:27:48.950", "88", "abcdefg", "20210830", "2021-08-20"));
        clicks.add(Arrays.asList("69", "用户E", "http://127.0.0.1/api/K", "2019-07-23 23:27:53.960", "88", "abcdefg", "20210830", "2021-08-20"));
        clicks.add(Arrays.asList("67", "用户C", "http://127.0.0.1/api/J", "2019-07-23 23:28:02.990", "88", "abcdefg", "20210830", "2021-08-24"));
        clicks.add(Arrays.asList("65", "用户A", "http://127.0.0.1/api/K", "2019-07-23 23:27:30.960", "88", "abcdefg", "20210830", "2021-08-20"));//晚到数据
        clicks.add(Arrays.asList("69", "用户E", "http://127.0.0.1/api/K", "2019-07-23 23:28:08.012", "88", "abcdefg", "20210830", "2021-08-20"));
        clicks.add(Arrays.asList("66", "用户B", "http://127.0.0.1/api/K", "2019-07-23 23:28:12.029", "88", "abcdefg", "20210830", "2021-08-24"));
        clicks.add(Arrays.asList("69", "用户E", "http://127.0.0.1/api/I", "2019-07-23 23:27:39.918", "88", "abcdefg", "20210830", "2021-08-20"));//晚到数据
        clicks.add(Arrays.asList("67", "用户C", "http://127.0.0.1/api/K", "2019-07-23 23:27:43.931", "88", "abcdefg", "20210830", "2021-08-21"));//晚到数据
        clicks.add(Arrays.asList("66", "用户B", "http://127.0.0.1/api/K", "2019-07-23 23:28:20.000", "88", "abcdefg", "20210830", "2021-08-23"));
        clicks.add(Arrays.asList("67", "用户C", "http://127.0.0.1/api/K", "2019-07-23 23:28:20.001", "88", "abcdefg", "20210830", "2021-08-23"));
        clicks.add(Arrays.asList("67", "用户C", "http://127.0.0.1/api/O", "2019-07-23 23:28:20.581", "88", "abcdefg", "20210830", "2021-08-20"));
    }

    @Override
    public void run(SourceContext<Row> sourceContext) throws Exception {
        Iterator<List<String>> iterator = clicks.iterator();
        while (iterator.hasNext()) {
            List<String> record = iterator.next();
            Row row = new Row(8);
            row.setField(0, Integer.parseInt(record.get(0)));
            row.setField(1, record.get(1));
            row.setField(2, record.get(2));
            row.setField(3, Timestamp.valueOf(record.get(3)));
            row.setField(4, Integer.parseInt(record.get(4)));
            row.setField(5, record.get(5));
            row.setField(6, record.get(6));
            row.setField(7, record.get(7));
            logger.info("emit {} -> {}", Timestamp.valueOf(record.get(3)).getTime(), row);
            sourceContext.collect(row);

            Thread.sleep(1000 * 1);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
