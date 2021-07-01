package com.flink.demo.cases.case24;

import com.flink.demo.cases.common.datasource.AllDataTypeDataSource;
import org.apache.avro.Schema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.types.Row;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.StreamWriteOperatorFactory;
import org.apache.hudi.sink.compact.*;
import org.apache.hudi.sink.partitioner.BucketAssignFunction;

import java.util.Objects;

public class HudiSink_COW {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000 * 5);

        RowTypeInfo rowTypeInfo = (RowTypeInfo) AllDataTypeDataSource.allDataTypeInfo;

        DataStream<Row> allDataTypeStream = env.addSource(new AllDataTypeDataSource())
                .returns(rowTypeInfo)
                .name("all data type source");

        Schema schema = AvroSchemaConverter.convertToSchema(rowTypeInfo);

        allDataTypeStream.printToErr("all_data_type_sink");

        Configuration conf = new Configuration();
        conf.setString(FlinkOptions.PATH, "file:///tmp/shiy/spark/flink-hudi-sink-cow");
        conf.setString(FlinkOptions.READ_AVRO_SCHEMA, schema.toString());
        conf.setString(FlinkOptions.TABLE_NAME, "TestHoodieTable");
        conf.setString(FlinkOptions.PARTITION_PATH_FIELD, "ts");
        conf.setString(FlinkOptions.RECORD_KEY_FIELD, "int_col");
        conf.setDouble(FlinkOptions.WRITE_BATCH_SIZE, 0.01);
        StreamWriteOperatorFactory<HoodieRecord> operatorFactory =
                new StreamWriteOperatorFactory<>(conf);

        SingleOutputStreamOperator<HoodieRecord> hoodieRecords = allDataTypeStream
                .map(new RowToHoodieFunction(rowTypeInfo, conf))
                .returns(HoodieRecord.class);
        hoodieRecords.printToErr("hoodie record sink");

        hoodieRecords.keyBy(HoodieRecord::getPartitionPath)
                .transform(
                        "bucket_assigner",
                        TypeInformation.of(HoodieRecord.class),
                        new KeyedProcessOperator<>(new BucketAssignFunction<>(conf)))
                .uid("uid_bucket_assigner")
                // shuffle by fileId(bucket id)
                .keyBy(record -> record.getCurrentLocation().getFileId())
                .transform("hoodie_stream_write", null, operatorFactory)
                .uid("uid_hoodie_stream_write");

        env.executeAsync("hudi sink training");

    }

}
