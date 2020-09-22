package com.flink.demo.cases.case10.count;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.types.Row;

public class CountWindowAgg implements AggregateFunction<Row, Long, Row> {

    @Override
    public Long createAccumulator() {
        return new Long(0);
    }

    @Override
    public Long add(Row value, Long accumulator) {
        return ++accumulator;
    }

    @Override
    public Row getResult(Long accumulator) {
        Row result = new Row(1);
        result.setField(0, accumulator);
        return result;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}
