package com.flink.demo.cases.case17;

import org.apache.flink.table.functions.AggregateFunction;

public class ConcatAggFunction extends AggregateFunction<String, StringBuilder> {

    @Override
    public StringBuilder createAccumulator() {
        return new StringBuilder();
    }

    public void accumulate(StringBuilder acc, String separator, String colValue) {
        if (acc.length() == 0) {
            acc.append(colValue);
        } else {
            acc.append(separator).append(colValue);
        }
    }

    @Override
    public String getValue(StringBuilder acc) {
        return acc.toString();
    }
}
