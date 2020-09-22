package com.flink.demo.cases.case06;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.types.Row;

/**
 * Created by P0007 on 2019/10/9.
 */
public class FlinkAggregateFunction implements AggregateFunction<Row, AggAccumulator, Row> {

    @Override
    public AggAccumulator createAccumulator() {
        return new AggAccumulator();
    }

    @Override
    public AggAccumulator add(Row value, AggAccumulator accumulator) {
        accumulator.inc();
        return accumulator;
    }

    @Override
    public Row getResult(AggAccumulator accumulator) {
        Row row = new Row(1);
        row.setField(0, accumulator.getCount());
        return row;
    }

    @Override
    public AggAccumulator merge(AggAccumulator a, AggAccumulator b) {
        return a;
    }

}
