package com.flink.demo.cases.case24;

import com.flink.demo.cases.common.datasource.AllDataTypeDataSource;
import org.apache.avro.Schema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.types.Row;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.*;
import org.apache.hudi.sink.partitioner.BucketAssignFunction;
import org.apache.hudi.streamer.FlinkStreamerConfig;
import org.apache.hudi.util.StreamerUtil;

import java.util.List;
import java.util.Objects;

/**
 *
 * 旧方式写入hudi
 */
public class HudiSinkV1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000 * 10);

        RowTypeInfo rowTypeInfo = (RowTypeInfo) AllDataTypeDataSource.allDataTypeInfo;

        DataStream<Row> allDataTypeStream = env.addSource(new AllDataTypeDataSource())
                .returns(rowTypeInfo)
                .name("all data type source");

        Schema schema = AvroSchemaConverter.convertToSchema(rowTypeInfo);

        allDataTypeStream.printToErr("all_data_type_sink");

        FlinkStreamerConfig streamerConf = new FlinkStreamerConfig();
        streamerConf.targetBasePath = "file:///tmp/shiy/spark/flink-hudi-sink-v2";
        streamerConf.readSchemaFilePath = Objects.requireNonNull(Thread.currentThread()
                .getContextClassLoader().getResource("test_read_schema.avsc")).toString();
        streamerConf.targetTableName = "TestHoodieTable";
        streamerConf.recordKeyField = "int_col";
        streamerConf.partitionPathField = "ts";
        streamerConf.tableType = "COPY_ON_WRITE";
        streamerConf.checkpointInterval = 4000L;
        streamerConf.setDouble(FlinkOptions.WRITE_BATCH_SIZE, 0.01);

        Configuration conf = FlinkOptions.fromStreamerConfig(streamerConf);
        env.getConfig().setGlobalJobParameters(streamerConf);

//        Configuration conf = new Configuration();
//        conf.setString(FlinkOptions.PATH, "file:///tmp/shiy/spark/flink-hudi-sink");
//        conf.setString(FlinkOptions.READ_AVRO_SCHEMA, schema.toString());
//        conf.setString(FlinkOptions.TABLE_NAME, "TestHoodieTable");
//        conf.setString(FlinkOptions.PARTITION_PATH_FIELD, "ts");
//        conf.setString(FlinkOptions.RECORD_KEY_FIELD, "int_col");
//        conf.setDouble(FlinkOptions.WRITE_BATCH_SIZE, 0.01);
//        StreamWriteOperatorFactory<HoodieRecord> operatorFactory =
//                new StreamWriteOperatorFactory<>(conf);

        StreamerUtil.initTableIfNotExists(conf);

        SingleOutputStreamOperator<HoodieRecord> hoodieRecords = allDataTypeStream
                .map(new RowToHoodieFunction(rowTypeInfo, conf)).returns(HoodieRecord.class);
        hoodieRecords.printToErr("hoodie record sink");

        hoodieRecords
                .transform(InstantGenerateOperator.NAME, TypeInformation.of(HoodieRecord.class), new InstantGenerateOperator())
                .name("instant_generator")
                .uid("instant_generator_id")
                .keyBy(HoodieRecord::getPartitionPath)
                .transform(
                        "bucket_assigner",
                        TypeInformation.of(HoodieRecord.class),
                        new KeyedProcessOperator<>(new BucketAssignFunction<>(conf)))
                .uid("uid_bucket_assigner")
                // shuffle by fileId(bucket id)
                .keyBy(record -> record.getCurrentLocation().getFileId())
                // write operator, where the write operation really happens
                .transform(KeyedWriteProcessOperator.NAME, TypeInformation.of(new TypeHint<Tuple3<String, List<WriteStatus>, Integer>>() {
                }), new KeyedWriteProcessOperator(new KeyedWriteProcessFunction()))
                .name("write_process")
                .uid("write_process_uid")
                .setParallelism(4)

                // Commit can only be executed once, so make it one parallelism
                .addSink(new CommitSink())
                .name("commit_sink")
                .uid("commit_sink_uid")
                .setParallelism(1);

        env.executeAsync("hudi sink training");

    }

}
