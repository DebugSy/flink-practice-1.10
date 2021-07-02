package com.flink.demo.cases.case25;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.types.Row;

import static com.flink.demo.cases.case25.MetricsNames.*;

/**
 * Metrics Counter
 * 主要用于计数功能
 * 如：
 * 1. 读取数据条数,写出数据条数
 * 2. 读取数据字节数,写出数据字节数
 *
 */
public class CounterTrainFunction extends RichMapFunction<Row, Row> {

    private Counter inputCnt;

    @Override
    public void open(Configuration parameters) throws Exception {
        RuntimeContext runtimeContext = this.getRuntimeContext();
        MetricGroup metricGroup = runtimeContext.getMetricGroup().addGroup(METRIC_GROUP);
        this.inputCnt = metricGroup.counter(INPUT_RECORDS);
    }

    @Override
    public Row map(Row value) throws Exception {
        inputCnt.inc();
        return value;
    }
}
