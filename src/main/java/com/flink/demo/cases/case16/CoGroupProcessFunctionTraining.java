package com.flink.demo.cases.case16;

import com.flink.demo.cases.common.datasource.FileUrlClickRowDataSource;
import com.flink.demo.cases.common.datasource.FileUserRowDataSource;
import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import com.flink.demo.cases.common.datasource.UserRowDataSource;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 *
 */
public class CoGroupProcessFunctionTraining {

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

        DataStream<Row> dataStream = clickStreamAndWatermarks.connect(userStreamAndWatermarks)
                .keyBy("userId", "userId")
                .process(new KeyedCoProcessFunction<Object, Row, Row, Row>() {

                    private transient ValueState<Row> leftCache;

                    private transient ValueState<Row> rightCache;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Row> leftCacheDesc = new ValueStateDescriptor<>("left_cache", TypeInformation.of(Row.class));
                        leftCache = getRuntimeContext().getState(leftCacheDesc);

                        ValueStateDescriptor<Row> rightCacheDesc = new ValueStateDescriptor<>("right_cache", TypeInformation.of(Row.class));
                        rightCache = getRuntimeContext().getState(rightCacheDesc);
                    }

                    @Override
                    public void processElement1(Row value, Context ctx, Collector<Row> out) throws Exception {
                        Row userCache = rightCache.value();
                        if (userCache != null) {
                            Row result = Row.join(value, userCache);
                            out.collect(result);
                        } else {
                            leftCache.update(value);
                            Long timestamp = ctx.timestamp();
                            ctx.timerService().registerEventTimeTimer(timestamp + 1000 * 5);
                        }
                    }

                    @Override
                    public void processElement2(Row value, Context ctx, Collector<Row> out) throws Exception {
                        Row clickCache = leftCache.value();
                        if (clickCache != null) {
                            Row result = Row.join(clickCache, value);
                            out.collect(result);
                        } else {
                            leftCache.update(value);
                            Long timestamp = ctx.timestamp();
                            ctx.timerService().registerEventTimeTimer(timestamp + 1000 * 5);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Row> out) throws Exception {
                        if (leftCache.value() != null) {
                            leftCache.clear();
                        }

                        if (rightCache.value() != null) {
                            rightCache.clear();
                        }
                    }
                });

        dataStream.printToErr();

        env.execute("Flink Stream CoGroup Training");
    }

}
