package com.flink.demo.cases.common.datasource;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.expressions.In;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Random;

/**
 * Created by DebugSy on 2019/7/15.
 *
 * 用户点击网页事件数据源
 */
public class UrlClickDataSource extends RichParallelSourceFunction<Tuple4<Integer, String, String, Timestamp>> {

    private static final Logger logger = LoggerFactory.getLogger(UrlClickDataSource.class);

    private volatile boolean running = true;

    public static String CLICK_FIELDS = "userId,username,url,clickTime";

    public static String CLICK_FIELDS_WITH_ROWTIME = "userId,username,url,clickTime,clickActionTime.rowtime";

    /*
    * metrics
    * */
    private Counter counter;

    @Override
    public void open(Configuration parameters) throws Exception {
        MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup("custom_group");
        Counter outputCnt = metricGroup.counter("output_cnt");
        this.counter = outputCnt;
    }

    @Override
    public void run(SourceContext<Tuple4<Integer, String, String, Timestamp>> ctx) throws Exception {
        Random random = new Random(System.currentTimeMillis());
        long count = 0;
        while (running) {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
//            logger.info("The index of the parallel subtask is {}", indexOfThisSubtask);
//            Thread.sleep((indexOfThisSubtask + 1) * 1000);
            if (count != 0 && count % 10 == 0) {
                logger.info("Sleep 10s");
                Thread.sleep(1000 * 10);
            } else {
                Thread.sleep((indexOfThisSubtask + 1) * 1000);
            }
            int nextInt = random.nextInt(10);
            Integer userId = 65;
            String username = "用户A";
            Timestamp clickTime = new Timestamp(System.currentTimeMillis());
            String url = "http://127.0.0.1/api/" + (char) ('H' + random.nextInt(4));
            Tuple4<Integer, String, String, Timestamp> tuple4 = new Tuple4<>(userId, username, url, clickTime);
            count++;
            logger.info("emit -> {}, count is {}", tuple4, count);
//                ctx.collectWithTimestamp(tuple3, clickTime.getTime());
//                ctx.emitWatermark(new Watermark(clickTime.getTime()));
            counter.inc();
            ctx.collect(tuple4);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

}
