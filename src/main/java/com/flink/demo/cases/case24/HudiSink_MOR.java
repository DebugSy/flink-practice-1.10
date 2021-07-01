package com.flink.demo.cases.case24;

import com.flink.demo.cases.common.datasource.AllDataTypeDataSource;
import org.apache.avro.Schema;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.types.Row;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.StreamWriteOperatorFactory;
import org.apache.hudi.sink.compact.*;
import org.apache.hudi.sink.partitioner.BucketAssignFunction;

public class HudiSink_MOR {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000 * 4, CheckpointingMode.EXACTLY_ONCE);

        RowTypeInfo rowTypeInfo = (RowTypeInfo) AllDataTypeDataSource.allDataTypeInfo;

        DataStream<Row> allDataTypeStream = env.addSource(new AllDataTypeDataSource())
                .returns(rowTypeInfo)
                .name("all data type source");

        Schema schema = AvroSchemaConverter.convertToSchema(rowTypeInfo);

        allDataTypeStream.printToErr("all_data_type_sink");

        Configuration conf = new Configuration();
        conf.setString(FlinkOptions.PATH, "file:///tmp/shiy/spark/flink-hudi-sink-mor");
        conf.setString(FlinkOptions.READ_AVRO_SCHEMA, schema.toString());
        conf.setString(FlinkOptions.TABLE_NAME, "TestHoodieTable");
        conf.setString(FlinkOptions.PARTITION_PATH_FIELD, "ts");
        conf.setString(FlinkOptions.RECORD_KEY_FIELD, "int_col");
//        conf.setDouble(FlinkOptions.WRITE_BATCH_SIZE, 0.01);
        conf.setInteger(FlinkOptions.COMPACTION_DELTA_COMMITS, 1);
        conf.setString(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());
        StreamWriteOperatorFactory<HoodieRecord> operatorFactory =
                new StreamWriteOperatorFactory<>(conf);

        SingleOutputStreamOperator<HoodieRecord> hoodieRecords = allDataTypeStream
                .map(new RowToHoodieFunction(rowTypeInfo, conf), TypeInformation.of(HoodieRecord.class));
        hoodieRecords.printToErr("hoodie record sink");

        hoodieRecords.keyBy(HoodieRecord::getPartitionPath)
                .transform(
                        "bucket_assigner",
                        TypeInformation.of(HoodieRecord.class),
                        new KeyedProcessOperator<>(new BucketAssignFunction<>(conf)))
                .uid("uid_bucket_assigner")
                // shuffle by fileId(bucket id)
                .keyBy(record -> record.getCurrentLocation().getFileId())
                .transform("hoodie_stream_write", TypeInformation.of(Object.class), operatorFactory)
                .uid("uid_hoodie_stream_write")
                .transform("compact_plan_generate",
                        TypeInformation.of(CompactionPlanEvent.class),
                        new CompactionPlanOperator(conf))
                .uid("uid_compact_plan_generate")
                .setParallelism(1) // plan generate must be singleton
                .keyBy(event -> event.getOperation().hashCode())
                .transform("compact_task",
                        TypeInformation.of(CompactionCommitEvent.class),
                        new KeyedProcessOperator<>(new CompactFunction(conf)))
                .addSink(new CompactionCommitSink(conf))
                .name("compact_commit")
                .setParallelism(1);

        env.executeAsync("hudi sink training");

    }

}
