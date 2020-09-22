package com.flink.demo.cases.case08;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

public class StreamingFileSinkOfRow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(5));

        RowTypeInfo rowTypeInfo = (RowTypeInfo) UrlClickRowDataSource.USER_CLICK_TYPEINFO;

        SingleOutputStreamOperator source = env.addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO)
                .name("url click source");

        SingleOutputStreamOperator watermarks = source.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row row) {
                return Timestamp.valueOf(row.getField(3).toString()).getTime();
            }
        });

        watermarks.printToErr();

        EventTimeBucketAssigner eventTimeBucketAssigner = new EventTimeBucketAssigner("yyyyMMddHHmm", ZoneId.systemDefault());

        Path path = new Path("E://tmp/shiy/flink/streamingfilesink");
        StreamingFileSink sink = StreamingFileSink
                .forRowFormat(path, new SimpleStringEncoder<Row>())
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(1))
                        .withInactivityInterval(TimeUnit.SECONDS.toMillis(30))
                        .withMaxPartSize(1024 * 1024 * 1024)
                        .build())
                .withBucketAssigner(eventTimeBucketAssigner)
                .withBucketCheckInterval(1000 * 5)
                .build();

        watermarks.addSink(sink);

        env.execute();
    }

}
