package com.flink.demo.cases.case26;

import com.flink.demo.cases.case08.AnyTimeBucketAssigner;
import com.flink.demo.cases.case08.EventTimeBucketAssigner;
import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import lombok.extern.slf4j.Slf4j;
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

@Slf4j
public class StreamingFileSinkMain {

    private final static int parallelism = 5;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
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

//        AnyTimeBucketAssigner bucketAssigner = new AnyTimeBucketAssigner(
//                "yyyyMMddHH", ZoneId.systemDefault(),
//                60);
        EventTimeBucketAssigner bucketAssigner = new EventTimeBucketAssigner("yyyyMMddHH", ZoneId.systemDefault());


        Path path = new Path("E://tmp/shiy/flink/streamingfilesink");
        StreamingFileSink.DefaultRowFormatBuilder bucketBuilder = StreamingFileSink
                .forRowFormat(path, new SimpleStringEncoder<Row>())
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withRolloverInterval(TimeUnit.SECONDS.toMillis(10))
                        .withInactivityInterval(TimeUnit.SECONDS.toMillis(10))
                        .withMaxPartSize(1024 * 1024 * 1024)
                        .build())
                .withBucketAssigner(bucketAssigner)
                .withBucketCheckInterval(1000 * 3);

        StreamingFileSinkOperator<Row, String> sinkOperator = new StreamingFileSinkOperator<>(
                bucketBuilder,
                bucketBuilder.getBucketCheckInterval(),
                TimeUnit.MINUTES.toMillis(60) +
                        TimeUnit.SECONDS.toMillis(5) +
                        TimeUnit.SECONDS.toMillis(10) +
                        TimeUnit.SECONDS.toMillis(3));

        // sink to hdfs and emit bucket inactive message
        SingleOutputStreamOperator writerStream = watermarks.transform(
                "StreamingFileSink",
                TypeInformation.of(BucketEvent.class),
                sinkOperator).setParallelism(parallelism);

        // collect bucket inactive message notify the register listener
        StreamingFileBucketCommitter<String> committer = new StreamingFileBucketCommitter<>(new BucketListener<String>() {
            @Override
            public void notifyBucketInactive(BucketInfo<String> bucketInfo) {
                System.err.println(String.format("bucket inactive: %s", bucketInfo));
                log.info("[RESULT] bucket inactive: {}", bucketInfo);
            }
        });
        writerStream.transform("StreamingFileSinkCommitter", Types.VOID, committer)
                .setParallelism(1)
                .setMaxParallelism(1);

        env.execute();
    }

}
