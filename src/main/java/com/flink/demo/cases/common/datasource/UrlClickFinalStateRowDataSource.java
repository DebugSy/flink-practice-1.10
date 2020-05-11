package com.flink.demo.cases.common.datasource;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

/**
 * Created by P0007 on 2019/9/3.
 */
public class UrlClickFinalStateRowDataSource extends RichParallelSourceFunction<Row> {

    private static final Logger logger = LoggerFactory.getLogger(UrlClickDataSource.class);

    private volatile boolean running = true;

    public static TypeInformation USER_CLICK_TYPEINFO = Types.ROW(
            new String[]{"userId", "username", "url", "clickTime", "random", "uuid", "data_col", "time_col", "final_state"},
            new TypeInformation[]{
                    Types.INT(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.SQL_TIMESTAMP(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING()
            });

    public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    public static SimpleDateFormat timeFormat = new SimpleDateFormat("HHmmss");

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        Random random = new Random(System.currentTimeMillis());
        int i = random.nextInt(10);
        while (running) {
            int dataType = i % 2;
            Row row;
            switch (dataType) {
                case 0:
                    row = genarateRow1(random);
                    break;
                case 1:
                    row = genarateRow2(random);
                    break;
                default:
                    throw new RuntimeException("Not support data type " + dataType);
            }
            logger.info("emit -> {}", row);
            ctx.collect(row);
        }
    }

    private Row genarateRow1(Random random) throws InterruptedException {
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        Thread.sleep((indexOfThisSubtask + 1) * 10);
        int nextInt = random.nextInt(5);
        Integer userId = nextInt;
        String username = "user" + (char) ('A' + nextInt);
        String url = "http://www.inforefiner.com/api/" + (char) ('H' + random.nextInt(4));
        Timestamp clickTime = new Timestamp(System.currentTimeMillis() - 7171000);//往前倒2小时
        Date date = new Date(clickTime.getTime());
        String dateStr = dateFormat.format(date);
        String timeStr = timeFormat.format(date);
        Row row = new Row(7);
        row.setField(0, userId);
        row.setField(1, username);
        row.setField(2, null);
        row.setField(3, clickTime);
        row.setField(4, dateStr);
        row.setField(5, null);
        row.setField(6, null);
        return row;
    }

    private Row genarateRow2(Random random) throws InterruptedException {
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        Thread.sleep((indexOfThisSubtask + 1) * 10);
        int nextInt = random.nextInt(5);
        Integer userId = nextInt;
        String username = "user" + (char) ('A' + nextInt);
        String url = "http://www.inforefiner.com/api/" + (char) ('H' + random.nextInt(4));
        Timestamp clickTime = new Timestamp(System.currentTimeMillis() - 7171000);//往前倒2小时
        Date date = new Date(clickTime.getTime());
        String dateStr = dateFormat.format(date);
        String timeStr = timeFormat.format(date);
        Row row = new Row(7);
        row.setField(0, userId);
        row.setField(1, username);
        row.setField(2, url);
        row.setField(3, clickTime);
        row.setField(4, null);
        row.setField(5, timeStr);
        row.setField(6, timeStr);
        return row;
    }

    @Override
    public void cancel() {
        running = false;
    }

}

