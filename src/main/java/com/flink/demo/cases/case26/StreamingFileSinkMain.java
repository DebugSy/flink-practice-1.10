package com.flink.demo.cases.case26;

import com.flink.demo.cases.case08.EventTimeBucketAssigner;
import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
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

public class StreamingFileSinkMain {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(5));
        env.setRestartStrategy(RestartStrategies.noRestart());

        SingleOutputStreamOperator source = env.addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO)
                .name("url click source");

        SingleOutputStreamOperator watermarks = source.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row row) {
                return Timestamp.valueOf(row.getField(3).toString()).getTime();
            }
        });

        EventTimeBucketAssigner eventTimeBucketAssigner = new EventTimeBucketAssigner("yyyyMMddHHmm", ZoneId.systemDefault());

        Path path = new Path("E://tmp/shiy/flink/streamingfilesink");
        StreamingFileSink.DefaultRowFormatBuilder bucketBuilder = StreamingFileSink
                .forRowFormat(path, new SimpleStringEncoder<Row>())
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(1))
                        .withInactivityInterval(TimeUnit.SECONDS.toMillis(30))
                        .withMaxPartSize(1024 * 1024 * 1024)
                        .build())
                .withBucketAssigner(eventTimeBucketAssigner)
                .withBucketCheckInterval(1000 * 5);

        StreamingFileSinkOperator<Row, String> sinkOperator = new StreamingFileSinkOperator<>(
                bucketBuilder,
                bucketBuilder.getBucketCheckInterval());

        // sink to hdfs and emit bucket inactive message
        SingleOutputStreamOperator writerStream = watermarks.transform(
                "StreamingFileSink",
                TypeInformation.of(BucketEvent.class),
                sinkOperator).setParallelism(5);

        // collect bucket inactive message notify the register listener
        StreamingFileBucketCommitter<String> committer = new StreamingFileBucketCommitter<>(new BucketListener<String>() {
            @Override
            public void notifyBucketInactive(BucketInfo<String> bucketInfo) {
                System.err.println(String.format("bucket inactive: %s", bucketInfo));
            }
        });
        writerStream.transform("StreamingFileSinkCommitter", Types.VOID, committer)
                .setParallelism(1)
                .setMaxParallelism(1);

        env.execute();
    }

}
