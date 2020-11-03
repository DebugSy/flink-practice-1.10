package com.flink.demo.cases.case16;

import com.flink.demo.cases.common.datasource.FileUrlClickRowDataSource;
import com.flink.demo.cases.common.datasource.FileUserRowDataSource;
import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import com.flink.demo.cases.common.datasource.UserRowDataSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.util.keys.KeySelectorUtil;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import javax.xml.crypto.KeySelector;
import java.sql.Timestamp;

/**
 *
 */
public class IntervalJoinTraining {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Row> clickStream = env.addSource(new FileUrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO);
        SingleOutputStreamOperator<Row> clickStreamAndWatermarks = clickStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row element) {
                return Timestamp.valueOf(element.getField(3).toString()).getTime();
            }
        });

        SingleOutputStreamOperator<Row> userStream = env.addSource(new FileUserRowDataSource())
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

        KeyedStream<Row, Tuple> clickKeyedStream = clickStreamAndWatermarks.keyBy("userId");
        KeyedStream<Row, Tuple> userKeyedStream = userStreamAndWatermarks.keyBy("userId");
        DataStream<Row> dataStream = clickKeyedStream
                .intervalJoin(userKeyedStream)
                .between(Time.seconds(0), Time.seconds(5))
                .process(new ProcessJoinFunction<Row, Row, Row>() {
                    @Override
                    public void processElement(Row left, Row right, Context ctx, Collector<Row> out) throws Exception {
                        Row result = Row.join(left, right);
                        out.collect(result);
                    }
                });

        dataStream.printToErr();

        env.execute("Flink Stream Interval Join Training");
    }

}
