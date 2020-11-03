package com.flink.demo.cases.case20;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.*;

public class WindowSortFunction extends ProcessWindowFunction<Row, Row, Tuple, TimeWindow> {

    private final Comparator<Row> comparator;

    private transient List<Row> rows;

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public WindowSortFunction(Comparator<Row> comparator) {
        this.comparator = comparator;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.rows = new LinkedList<>();
    }

    @Override
    public void process(Tuple o, Context context, Iterable<Row> elements, Collector<Row> out) throws Exception {
        for (Row element : elements) {
            rows.add(element);
        }

        Collections.sort(rows, comparator);
        Iterator<Row> iterator = rows.iterator();
        int rank = 1;
        Row rankCol = new Row(3);
        while (iterator.hasNext()) {
            Row row = iterator.next();
            rankCol.setField(0, rank++);
            rankCol.setField(1, dateFormat.format(new Date(context.window().getStart())));
            rankCol.setField(2, dateFormat.format(new Date(context.window().getEnd())));
            Row result = Row.join(row, rankCol);
            out.collect(result);
        }
    }

    @Override
    public void clear(Context context) throws Exception {
        rows.clear();
    }
}
