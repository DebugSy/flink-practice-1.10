package com.flink.demo.cases.case09;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

public class FlinkReduceStateTraining {

    public static TypeInformation STATE_ACC_TYPE_INFO = Types.ROW_NAMED(
            new String[]{"userId", "username", "url", "clickTime", "data_col", "time_col", "userId_sum"},
            new TypeInformation[]{
                    Types.INT,
                    Types.STRING,
                    Types.STRING,
                    Types.SQL_TIMESTAMP,
                    Types.STRING,
                    Types.STRING,
                    Types.LONG
            });

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(5));

        RowTypeInfo rowTypeInfo = (RowTypeInfo) UrlClickRowDataSource.USER_CLICK_TYPEINFO;

        SingleOutputStreamOperator source = env.addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO)
                .setParallelism(1)
                .name("url click source");

        SingleOutputStreamOperator watermarks = source.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row row) {
                return Timestamp.valueOf(row.getField(3).toString()).getTime();
            }
        });

        SingleOutputStreamOperator sink = watermarks
                .keyBy("userId")
                .process(new StateAccumulateFunction("m", 0L))
//                .returns(STATE_ACC_TYPE_INFO)
                .setParallelism(6);

        sink.printToErr().setParallelism(1);

        env.execute();
    }

}
