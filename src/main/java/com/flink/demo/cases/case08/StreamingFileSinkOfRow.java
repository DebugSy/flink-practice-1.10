package com.flink.demo.cases.case08;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
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

        BucketingSink<Row> bucketingSink = new BucketingSink<>("E://tmp/shiy/flink/streamingfilesink");
        bucketingSink.setBatchRolloverInterval(1000 * 10);
        DateTimeBucketer dateTimeBucketer = new DateTimeBucketer("yyyy-MM-dd-HH-mm");
        bucketingSink.setBucketer(dateTimeBucketer);
        watermarks.addSink(bucketingSink);

        env.execute();
    }

}
