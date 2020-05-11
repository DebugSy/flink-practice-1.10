package com.flink.demo.cases.common.datasource;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Random;

/**
 * Created by DebugSy on 2019/7/15.
 *
 * 用户活动事件数据源
 */
public class UserRowDataSource extends RichSourceFunction<Row> {

    private static final Logger logger = LoggerFactory.getLogger(UserRowDataSource.class);

    private volatile boolean running = true;

    public static String USER_FIELDS = "userId,username,address,activityTime";

    public static String USER_FIELDS_WITH_ROWTIME = "userId,username,address,activityTime";

    public static TypeInformation USER_TYPEINFO = Types.ROW(
            new String[]{"userId", "username", "address", "activityTime"},
            new TypeInformation[]{
                    Types.INT(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.SQL_TIMESTAMP()
            });

    private final Row row = new Row(4);

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        Random random = new Random(System.currentTimeMillis());
        while (running) {
            Thread.sleep(1000);
            int nextInt = random.nextInt(5);
            Integer userId = 65 + nextInt;
            String username = "用户" + (char) ('A' + nextInt);
            String address = "北京市朝阳区望京东湖街道" + nextInt + "号";
            Timestamp activityTime = new Timestamp(System.currentTimeMillis());

            row.setField(0, userId);
            row.setField(1, username);
            row.setField(2, address);
            row.setField(3, activityTime);
            logger.info("emit -> {}", row);
            //直接在数据流源中指定时间戳和水印
            ctx.collectWithTimestamp(row, activityTime.getTime());
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
