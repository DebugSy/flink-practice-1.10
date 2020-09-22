package com.flink.demo.cases.case10.count;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.runtime.aggregate.AggregateAggFunction;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

/**
 *
 * 自定义trigger实现最终状态触发窗口计算
 *
 */
@Slf4j
public class FlinkWindowTriggerTraining_Count {

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
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .trigger(ElementTriggers.count(2))
                .aggregate(new CountWindowAgg(), new CountWindowFunc());

        aggregateStream.printToErr();


        env.execute("Incremental Window Aggregation with ReduceFunction");

    }

}
