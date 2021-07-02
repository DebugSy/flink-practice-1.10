package com.flink.demo.cases.case25;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.types.Row;

import java.util.Random;

import static com.flink.demo.cases.case25.MetricsNames.*;
import static org.apache.flink.runtime.metrics.MetricNames.IO_NUM_RECORDS_IN;

/**
 * Metrics Meter
 * 主要用来计算平均速率，直接使用其子类MeterView更方便一些
 * flink中类似指标有task/operator中的numRecordsInPerSecond，字面意思就可以理解，指的是此task或者operator每秒接收的记录数。
 */
public class MeterTrainFunction extends RichMapFunction<Row, Row> {

    private transient Random random;

    private Counter numRecordsIn;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.random = new Random(System.currentTimeMillis());
        //metrics
        RuntimeContext runtimeContext = getRuntimeContext();
        MetricGroup metricGroup = runtimeContext.getMetricGroup().addGroup(METRIC_GROUP);
        numRecordsIn = metricGroup.counter(IO_NUM_RECORDS_IN);
        metricGroup.meter(PROCESSED_RECORD_RATE, new MeterView(numRecordsIn));
    }

    @Override
    public Row map(Row value) throws Exception {
        numRecordsIn.inc();
        int timeMillis = random.nextInt(100);
        Thread.sleep(timeMillis);
        return value;
    }
}
