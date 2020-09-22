package com.flink.demo.cases.case10;

import com.flink.demo.cases.case10.count.CountWindowAgg;
import com.flink.demo.cases.case10.count.CountWindowFunc;
import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

/**
 *
 * Count trigger
 *
 */
@Slf4j
public class WindowTrigger_Count {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Row> urlClickSource = env.addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO);

        SingleOutputStreamOperator<Row> streamSourceWithWatermarks = urlClickSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row element) {
                return Timestamp.valueOf(element.getField(3).toString()).getTime();
            }
        });

        SingleOutputStreamOperator<Row> aggregateStream = streamSourceWithWatermarks.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                .trigger(CountTrigger.of(3))
                .aggregate(new CountWindowAgg(), new CountWindowFunc());

        aggregateStream.printToErr();


        env.execute("Incremental Window Aggregation with ReduceFunction");

    }

}
