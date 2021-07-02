package com.flink.demo.cases.case25;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.types.Row;

import java.util.Random;

import static com.flink.demo.cases.case25.MetricsNames.METRIC_GROUP;
import static com.flink.demo.cases.case25.MetricsNames.PROCESS_TIME;

/**
 * Metrics Gauge测试
 * 用来记录一个metrics的瞬间值。
 * 拿flink中的指标举例，像JobManager或者TaskManager中的JVM.Heap.Used就属于Gauge，记录某个时刻JobManager或者TaskManager所在机器的JVM堆使用量。
 */
public class GaugeTrainFunction extends RichMapFunction<Row, Row> {

    private long processTime;

    private transient Random random;

    @Override
    public void open(Configuration parameters) throws Exception {
        random = new Random(System.currentTimeMillis());
        //metrics
        RuntimeContext runtimeContext = this.getRuntimeContext();
        MetricGroup metricGroup = runtimeContext.getMetricGroup().addGroup(METRIC_GROUP);
        metricGroup.gauge(PROCESS_TIME, () -> processTime);
    }

    @Override
    public Row map(Row value) throws Exception {
        long startTime = System.currentTimeMillis();
        int timeMillis = random.nextInt(50);
        Thread.sleep(timeMillis);
        long endTime = System.currentTimeMillis();
        this.processTime = endTime - startTime;
        return value;
    }
}
