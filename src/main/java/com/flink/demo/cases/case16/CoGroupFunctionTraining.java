package com.flink.demo.cases.case16;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import com.flink.demo.cases.common.datasource.UserRowDataSource;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 *
 */
public class CoGroupFunctionTraining {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Row> clickStream = env.addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO);
        SingleOutputStreamOperator<Row> clickStreamAndWatermarks = clickStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row element) {
                return Timestamp.valueOf(element.getField(3).toString()).getTime();
            }
        });

        SingleOutputStreamOperator<Row> userStream = env.addSource(new UserRowDataSource())
                .returns(UserRowDataSource.USER_TYPEINFO);
        SingleOutputStreamOperator<Row> userStreamAndWatermarks = userStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row element) {
                return Timestamp.valueOf(element.getField(3).toString()).getTime();
            }
        });

        int leftArity = clickStream.getType().getArity();
        int rightArity = userStream.getType().getArity();
        int arity = leftArity + rightArity;
        final Row result = new Row(arity);

        DataStream<Row> dataStream = clickStreamAndWatermarks.coGroup(userStreamAndWatermarks)
                .where(new KeySelector<Row, Object>() {
                    @Override
                    public Object getKey(Row value) throws Exception {
                        return value.getField(0);
                    }
                })
                .equalTo(new KeySelector<Row, Object>() {
                    @Override
                    public Object getKey(Row value) throws Exception {
                        return value.getField(0);
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<Row, Row, Row>() {
                    @Override
                    public void coGroup(Iterable<Row> first, Iterable<Row> second, Collector<Row> out) throws Exception {
                        for (Row left : first) {
                            for (Row right : second) {
                                int i = 0;
                                for (int j = 0; j < leftArity; j++) {
                                    result.setField(i++, left.getField(j));
                                }
                                for (int j = 0; j < rightArity; j++) {
                                    result.setField(i++, right.getField(j));
                                }
                                out.collect(result);
                            }
                        }
                    }
                });

        dataStream.printToErr();

        env.execute("Flink Stream CoGroup Training");
    }

}
