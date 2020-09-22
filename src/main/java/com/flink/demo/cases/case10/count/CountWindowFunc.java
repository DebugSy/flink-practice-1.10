package com.flink.demo.cases.case10.count;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Iterator;

public class CountWindowFunc implements WindowFunction<Row, Row, Tuple, TimeWindow> {

    private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private final Row row;

    public CountWindowFunc() {
        this.row = new Row(4);
    }

    @Override
    public void apply(Tuple key, TimeWindow window, Iterable<Row> input, Collector<Row> out) throws Exception {

        Iterator<Row> iterator = input.iterator();
        Row row = iterator.next();

        long windowStart = window.getStart();
        long windowEnd = window.getEnd();
        this.row.setField(0, key.getField(0));
        this.row.setField(1, row.getField(0));
        this.row.setField(2, format.format(new Date(windowStart)));
        this.row.setField(3, format.format(new Date(windowEnd)));
        out.collect(this.row);
    }



}
