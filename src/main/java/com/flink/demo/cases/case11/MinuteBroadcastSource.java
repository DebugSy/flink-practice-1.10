package com.flink.demo.cases.case11;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Slf4j
public class MinuteBroadcastSource extends RichParallelSourceFunction<Row> {

    private boolean isRunning = true;

    private List<String> cacheKeys = new ArrayList<>();

    private Random random = new Random(System.currentTimeMillis());

    private final Row row = new Row(1);

    public static RowTypeInfo rowTypeInfo = new RowTypeInfo(
            new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO},
            new String[]{"key_col"}
            );

    @Override
    public void open(Configuration parameters) throws Exception {
        for (int i = 0; i < 5; i++) {
            String key = "user" + (char) ('A' + i);
            cacheKeys.add(key);
        }
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        while (isRunning) {
            Thread.sleep(1000 * 10);
            int nextInt = random.nextInt(5);
            String key = cacheKeys.get(nextInt);
            row.setField(0, key);
            log.info("clean key {}", key);
            ctx.collect(row);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
