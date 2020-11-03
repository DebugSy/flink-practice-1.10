package com.flink.demo.cases.case20;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

public class WindowSortTraining {

    private static TypeInformation USER_CLICK_SORT_TYPEINFO = Types.ROW(
            new String[]{"userId", "username", "url", "clickTime", "rank_num", "uuid", "data_col", "time_col", "row_num", "window_start", "window_end"},
            new TypeInformation[]{
                    Types.INT(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.SQL_TIMESTAMP(),
                    Types.INT(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.INT(),
                    Types.STRING(),
                    Types.STRING()
            });

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(1000 * 5, CheckpointingMode.EXACTLY_ONCE);

        RowTypeInfo rowTypeInfo = (RowTypeInfo) UrlClickRowDataSource.USER_CLICK_TYPEINFO;

        SingleOutputStreamOperator<Row> streamSource = env.addSource(new UrlClickRowDataSource())
                .returns(rowTypeInfo)
                .name("url click source");

        SingleOutputStreamOperator<Row> streamSourceWithWatermarks = streamSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row element) {
                return Timestamp.valueOf(element.getField(3).toString()).getTime();
            }
        });

        RowColCompare compare = new RowColCompare(new int[]{0});

        SingleOutputStreamOperator sinkStream = streamSourceWithWatermarks.keyBy(0)
//                .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new WindowSortFunction(compare))
                .returns(USER_CLICK_SORT_TYPEINFO);

        sinkStream.printToErr();

        env.execute("flink row_number training");
    }

}
