package com.flink.demo.cases.common.datasource;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
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
public class UrlClickRowDataSource extends RichParallelSourceFunction<Row> {

    private static final Logger logger = LoggerFactory.getLogger(UrlClickDataSource.class);

    private volatile boolean running = true;

    public static String CLICK_FIELDS = "userId,username,url,clickTime,rank_col,uuid_col,data_col,time_col";

    public static String CLICK_FIELDS_WITH_ROWTIME = "userId,username,url,clickTime.rowtime,data_col,time_col";

    public static TypeInformation USER_CLICK_TYPEINFO = Types.ROW_NAMED(
            new String[]{"userId", "username", "url", "clickTime", "rank_col", "uuid_col", "data_col", "time_col"},
            new TypeInformation[]{
                    Types.INT,
                    Types.STRING,
                    Types.STRING,
                    Types.SQL_TIMESTAMP,
                    Types.INT,
                    Types.STRING,
                    Types.STRING,
                    Types.STRING
            });

    public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    public static SimpleDateFormat timeFormat = new SimpleDateFormat("HHmmss");

    private IntCounter intCounter = new IntCounter();

    @Override
    public void open(Configuration parameters) throws Exception {
        getRuntimeContext().addAccumulator("cnt", intCounter);
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        Random random = new Random(System.currentTimeMillis());
        int i = 0;
        while (running) {
            int dataType = i % 2;
            Row row;
            switch (dataType) {
                case 0:
//                    row = genarateRow1(random);
//                    break;
                case 1:
                    row = genarateRow2(random);
                    break;
                default:
                    throw new RuntimeException("Not support data type " + dataType);
            }
            logger.info("emit {}.th -> {}", intCounter.getLocalValue() + 1, row);
            ctx.collect(row);
            i++;

            intCounter.add(1);
            Thread.sleep(1000 * 10);
        }
    }

    private Row genarateRow1(Random random) throws InterruptedException {
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        Thread.sleep((indexOfThisSubtask + 1) * 10);
        int nextInt = random.nextInt(5);
        Integer userId = 65 + nextInt;
        String username = "user" + (char) ('A' + nextInt);
        String url = "http://www.inforefiner.com/api/" + (char) ('H' + random.nextInt(4));
        Timestamp clickTime = new Timestamp(System.currentTimeMillis() - 7171000);//往前倒2小时
        Integer rank = random.nextInt(100);
        String uuid = UUID.randomUUID().toString();
        Date date = new Date(clickTime.getTime());
        String dateStr = dateFormat.format(date);
        String timeStr = timeFormat.format(date);
        Row row = new Row(8);
        row.setField(0, username);
        row.setField(1, username);
        row.setField(2, url);
        row.setField(3, clickTime);
        row.setField(4, rank);
        row.setField(5, uuid);
        row.setField(6, null);
        row.setField(7, timeStr);
        return row;
    }

    private Row genarateRow2(Random random) throws InterruptedException {
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        Thread.sleep((indexOfThisSubtask + 1) * 10);
        int nextInt = random.nextInt(5);
        Integer userId = 65 + nextInt;
        String username = "user" + (char) ('A' + nextInt);
        String url = "http://www.inforefiner.com/api/" + (char) ('H' + random.nextInt(4));
        Timestamp clickTime = new Timestamp(System.currentTimeMillis() - 7171000);//往前倒2小时
        Integer rank = random.nextInt(100);
        String uuid = UUID.randomUUID().toString();
        Date date = new Date(clickTime.getTime());
        String dateStr = dateFormat.format(date);
        String timeStr = timeFormat.format(date);
        Row row = new Row(8);
        row.setField(0, userId);
        row.setField(1, username);
        row.setField(2, url);
        row.setField(3, clickTime);
        row.setField(4, rank);
        row.setField(5, uuid);
        row.setField(6, null);
        row.setField(7, timeStr);
        return row;
    }

    @Override
    public void cancel() {
        running = false;
    }

}

