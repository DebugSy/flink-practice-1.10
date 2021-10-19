package com.flink.demo.cases.case16;

import com.flink.demo.cases.common.datasource.FileUrlClickRowDataSource;
import com.flink.demo.cases.common.datasource.FileUserRowDataSource;
import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import com.flink.demo.cases.common.datasource.UserRowDataSource;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class JoinTraining {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Row> clickStream = env.addSource(new FileUrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO);
        SingleOutputStreamOperator<Row> leftStream = clickStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row element) {
                System.err.println("leftStream emit " + element.getField(1));
                return Timestamp.valueOf(element.getField(3).toString()).getTime();
            }
        });

        SingleOutputStreamOperator<Row> userStream = env.addSource(new FileUserRowDataSource())
                .returns(UserRowDataSource.USER_TYPEINFO);
        SingleOutputStreamOperator<Row> rightStream = userStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row element) {
                System.err.println("rightStream emit " + element.getField(1));
                return Timestamp.valueOf(element.getField(3).toString()).getTime();
            }
        });

        DataStream<Row> sinkStream = leftStream.coGroup(rightStream)
                .where(value -> value.getField(1))
                .equalTo(value -> value.getField(1))
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<Row, Row, Row>() {

                    final Row result = new Row(12);

                    @Override
                    public void coGroup(Iterable<Row> first, Iterable<Row> second, Collector<Row> out) throws Exception {
                        if (second == null || !second.iterator().hasNext()){
                            for (Row left : first){
                                // 右流中没有对应的记录
                                int i = 0;
                                for (int j = 0; j < 8; j++) {
                                    result.setField(i++, left.getField(j));
                                }
                                for (int j = 0; j < 4; j++) {
                                    result.setField(i++, null);
                                }
                                out.collect(result);
                            }
                        } else {
                            for (Row left : first) {
                                for (Row right : second) {
                                    boolean eval = left.getField(1) == right.getField(1);

                                    //if it matches, collect all, otherwise collect left table has no match
                                    int i = 0;
                                    if (eval) {
                                        for (int j = 0; j < 8; j++) {
                                            result.setField(i++, left.getField(j));
                                        }
                                        for (int j = 0; j < 4; j++) {
                                            result.setField(i++, right.getField(j));
                                        }
                                        out.collect(result);
                                    } else {
                                        for (int j = 0; j < 8; j++) {
                                            result.setField(i++, left.getField(j));
                                        }
                                        for (int j = 0; j < 4; j++) {
                                            result.setField(i++, null);
                                        }
                                        out.collect(result);
                                    }
                                }
                            }
                        }
                    }
                });

        sinkStream.printToErr();

        env.execute("Flink Stream Join Training");
    }

}
