package com.flink.demo.cases.case09;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.types.Row;

@Slf4j
public class StateAccReduceFunction extends RichReduceFunction<Row> {

    private final Row row = new Row(1);

    @Override
    public Row reduce(Row value1, Row value2) throws Exception {
        log.info("value1 is {}", value1);
        log.info("value2 is {}", value2);
        value1.getField(0);
        value1.getField(0);

        for (int i = 0; i < value1.getArity(); i++) {
            long cnt1 = Long.parseLong(value1.getField(i).toString());
            long cnt2 = Long.parseLong(value2.getField(i).toString());
            row.setField(i, cnt1 + cnt2);
        }
        return row;
    }

}
