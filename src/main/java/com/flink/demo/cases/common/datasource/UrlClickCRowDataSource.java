package com.flink.demo.cases.common.datasource;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Random;

/**
 * Created by P0007 on 2019/9/3.
 */
public class UrlClickCRowDataSource extends RichSourceFunction<CRow> {

    private static final Logger logger = LoggerFactory.getLogger(UrlClickDataSource.class);

    private volatile boolean running = true;

    public static String CLICK_FIELDS = "userId,username,url,clickTime";

    public static String CLICK_FIELDS_WITH_ROWTIME = "userId,username,url,clickTime.rowtime";

    @Override
    public void run(SourceContext<CRow> ctx) throws Exception {
        Random random = new Random(System.currentTimeMillis());
        while (running) {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            Thread.sleep((indexOfThisSubtask + 1) * 1000);
            int nextInt = random.nextInt(5);
            Integer userId = 65 + nextInt;
            String username = "user" + (char) ('A' + nextInt);
            String url = "http://www.inforefiner.com/api/" + (char) ('H' + random.nextInt(4));
            Timestamp clickTime = new Timestamp(System.currentTimeMillis());
            Row row = new Row(3);
            row.setField(0, username);
            row.setField(1, url);
            row.setField(2, clickTime.getTime());
            logger.info("emit -> {}", row);
            CRow cRow = new CRow(row, true);
            ctx.collect(cRow);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

}

