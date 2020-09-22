package com.flink.demo.cases.case08;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.avro.Schema;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

public class StreamingFileSinkOfBulk {

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

        Schema schema = new Schema
                .Parser()
                .parse("{\n" +
                        "\t\"type\": \"record\",\n" +
                        "\t\"name\": \"Row\",\n" +
                        "\t\"namespace\": \"org.apache.flink.avro.generated\",\n" +
                        "\t\"fields\": [\n" +
                        "\t\t{\n" +
                        "\t\t\t\"name\": \"userId\",\n" +
                        "\t\t\t\"type\": \"int\"\n" +
                        "\t\t},\n" +
                        "\t\t{\n" +
                        "\t\t\t\"name\": \"username\",\n" +
                        "\t\t\t\"type\": \"string\"\n" +
                        "\t\t},\n" +
                        "\t\t{\n" +
                        "\t\t\t\"name\": \"url\",\n" +
                        "\t\t\t\"type\": \"string\"\n" +
                        "\t\t},\n" +
                        "\t\t{\n" +
                        "\t\t\t\"name\": \"clickTime\",\n" +
                        "\t\t\t\"type\": \"long\"\n" +
                        "\t\t},\n" +
                        "\t\t{\n" +
                        "\t\t\t\"name\": \"data_col\",\n" +
                        "\t\t\t\"type\": \"string\"\n" +
                        "\t\t},\n" +
                        "\t\t{\n" +
                        "\t\t\t\"name\": \"time_col\",\n" +
                        "\t\t\t\"type\": \"string\"\n" +
                        "\t\t}\n" +
                        "\t]\n" +
                        "}");

        EventTimeBucketAssigner eventTimeBucketAssigner = new EventTimeBucketAssigner("yyyyMMddHHmm", ZoneId.systemDefault());

        ParquetBuilderImpl parquetBuilder = new ParquetBuilderImpl();


        Path path = new Path("E://tmp/shiy/flink/streamingfilesink_parquet");
        StreamingFileSink sink = StreamingFileSink
                .forBulkFormat(path, ParquetAvroWriters.forGenericRecord(schema))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
//                .withBucketAssigner(eventTimeBucketAssigner)
                .withBucketCheckInterval(1000 * 5)
                .build();

        watermarks.addSink(sink);

        env.execute();
    }

}
