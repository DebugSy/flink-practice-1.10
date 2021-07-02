package com.flink.demo.cases.case25;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.types.Row;

import java.util.Random;

import static com.flink.demo.cases.case25.MetricsNames.*;

/**
 * Metrics Histogram测试
 * 有的时候我们不满足于只拿到metrics的总量或者瞬时值，当想得到metrics的最大值，最小值，中位数等信息时，我们就能用到Histogram了
 * Flink中属于Histogram的指标很少，但是最重要的一个是属于operator的latency。此项指标会记录数据处理的延迟信息，对任务监控起到很重要的作用。
 */
public class HistogramTrainFunction extends RichMapFunction<Row, Row> {

    private transient Random random;

    private final int metricsHistorySize = 128;
    private Histogram histogram;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.random = new Random(System.currentTimeMillis());
        //metrics
        RuntimeContext runtimeContext = getRuntimeContext();
        MetricGroup metricGroup = runtimeContext.getMetricGroup().addGroup(METRIC_GROUP);
        CustomHistogram histogram = new CustomHistogram(metricsHistorySize);
        this.histogram = metricGroup.histogram(TOOK_TIME, histogram);
    }

    @Override
    public Row map(Row value) throws Exception {
        long startTime = System.currentTimeMillis();
        int timeMillis = random.nextInt(80);
        Thread.sleep(timeMillis);
        long endTime = System.currentTimeMillis();
        long processTime = endTime - startTime;
        histogram.update(processTime);
        return value;
    }
}
