package com.flink.demo.cases.case06;

import com.flink.demo.cases.common.datasource.OutOfOrderDataSource;
import com.flink.demo.cases.common.datasource.OutOfOrderRowDataSource;
import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.runtime.aggregate.AggregateAggFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;

/**
 * Created by P0007 on 2019/9/29.
 */
public class FlinkTrainingWatermark {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Row> streamSource = env
                .addSource(new OutOfOrderRowDataSource())
                .returns(OutOfOrderRowDataSource.CLICK_TYPEINFO);

        SingleOutputStreamOperator<Row> streamSourceWithWatermarks = streamSource
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(Row element) {
                        return Timestamp.valueOf(element.getField(3).toString()).getTime();
                    }
                });
        KeyedStream<Row, String> keyedStream = streamSourceWithWatermarks.keyBy(new KeySelector<Row, String>() {
            @Override
            public String getKey(Row value) throws Exception {
                return value.getField(1).toString();
            }
        });
        DataStream<Row> aggregateTraining = keyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(5))
                .aggregate(new FlinkAggregateFunction(), new WindowFunction<Row, Row, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Row> input, Collector<Row> out) throws Exception {
                        Row row = new Row(4);
                        row.setField(0, key);
                        Iterator<Row> iterator = input.iterator();
                        int i = 1;
                        while (iterator.hasNext()) {
                            Row next = iterator.next();
                            row.setField(i, next.getField(i - 1));
                            i++;
                        }
                        row.setField(2, new Timestamp(window.getStart()));
                        row.setField(3, new Timestamp(window.getEnd()));
                        out.collect(row);
                    }
                })
                .name("Flink Aggregate Training");

        aggregateTraining.printToErr();


        env.execute("flink job demo");
    }

}
