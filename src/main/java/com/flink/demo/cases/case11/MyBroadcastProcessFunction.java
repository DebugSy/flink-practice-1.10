package com.flink.demo.cases.case11;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

@Slf4j
public class MyBroadcastProcessFunction extends KeyedBroadcastProcessFunction<Tuple, Row, Row, Row> {

    private ValueState<Long> counter;

    private ReadOnlyBroadcastState<String, String> broadcastState;

    private final Row namespaceAndTable = new Row(2);

    private MapStateDescriptor<String, String> broadcastStateDesc = new MapStateDescriptor("broadcast",
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO);

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor stateDescriptor = new ValueStateDescriptor("counter", BasicTypeInfo.LONG_TYPE_INFO);
        this.counter = getRuntimeContext().getState(stateDescriptor);
    }

    @Override
    public void processElement(Row value, ReadOnlyContext ctx, Collector<Row> out) throws Exception {
        Long count = this.counter.value();
        if (count == null) {
            count = 0L;
        }
        if (broadcastState == null) {
            broadcastState = ctx.getBroadcastState(broadcastStateDesc);
        } else {
            Tuple currentKey = ctx.getCurrentKey();
            String broadcastKey = currentKey.getField(0).toString();
            String message = broadcastState.get(broadcastKey);
            log.info("currentKey is {}, broadcast state message is {}", currentKey, message);
            if (StringUtils.isNoneEmpty(message)) {
                log.info("clean broadcast state");
                this.counter.update(1L);
            } else {
                this.counter.update(count + 1);
            }
        }
        log.info("counter is {} , process element {}", this.counter.value(), value);

        namespaceAndTable.setField(0, "default");
        namespaceAndTable.setField(1, "shiy_table_" + value.getField(0));
        Row row = Row.join(namespaceAndTable, value);
        out.collect(row);
    }

    @Override
    public void processBroadcastElement(Row message, Context ctx, Collector<Row> out) throws Exception {
        log.info("process broadcast element, clean counter, message is {}", message);
        String key = message.getField(0).toString();
        BroadcastState<String, String> broadcastState = ctx.getBroadcastState(broadcastStateDesc);
        broadcastState.clear();
        broadcastState.put(key, key);
    }

}
