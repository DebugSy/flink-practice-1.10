package com.flink.demo.cases.case19;

import com.flink.demo.cases.common.datasource.FileUrlClickRowDataSource;
import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

/**
 * HBase写入失败溢出到HDFS保证数据吞吐量
 */
public class HBaseSpilloverHDFSTraining {

    public static TypeInformation TABLE_USER_CLICK_TYPEINFO = Types.ROW(
            new String[]{"tableName", "userId", "username", "url", "clickTime", "rank", "uuid", "data_col", "time_col"},
            new TypeInformation[]{
                    Types.STRING(),
                    Types.INT(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.SQL_TIMESTAMP(),
                    Types.INT(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING()
            });

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000 * 5, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("hdfs:///tmp/shiy/flink/checkpoints/"));

        RowTypeInfo rowTypeInfo = (RowTypeInfo) UrlClickRowDataSource.USER_CLICK_TYPEINFO;

        SingleOutputStreamOperator<Row> source = env.addSource(new UrlClickRowDataSource())
                .returns(rowTypeInfo)
                .name("url click source");

        SingleOutputStreamOperator tableNameSource = source.map(new RichMapFunction<Row, Row>() {

            private final Row row = new Row(1);

            @Override
            public Row map(Row value) throws Exception {
                String userId = value.getField(0).toString();
                row.setField(0, "shiy_table_" + userId);
                Row result = Row.join(row, value);
                return result;
            }
        }).returns(TABLE_USER_CLICK_TYPEINFO);

        Map<String, String> columnFamilyMap = new HashMap<>();
        columnFamilyMap.put("userId", "cf1");
        columnFamilyMap.put("url", "cf1");
        columnFamilyMap.put("clickTime", "cf1");
        columnFamilyMap.put("rank", "cf1");
        columnFamilyMap.put("uuid", "cf1");
        columnFamilyMap.put("data_col", "cf1");
        columnFamilyMap.put("time_col", "cf1");

        tableNameSource.addSink(new HBaseSpilloverHDFSSinkFunction(
                "tableName",
                2,
                columnFamilyMap,
                (RowTypeInfo) TABLE_USER_CLICK_TYPEINFO,
                2097152,
                50));

        env.execute("HBase custom sink training");
    }

}
