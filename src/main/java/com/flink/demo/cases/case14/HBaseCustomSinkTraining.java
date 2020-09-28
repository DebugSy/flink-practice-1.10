package com.flink.demo.cases.case14;

import com.flink.demo.cases.common.datasource.FileUrlClickRowDataSource;
import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

/**
 * 利用Broadcast Stream实现控制作用输出到HBase的表名
 */
public class HBaseCustomSinkTraining {

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

        RowTypeInfo rowTypeInfo = (RowTypeInfo) UrlClickRowDataSource.USER_CLICK_TYPEINFO;

        SingleOutputStreamOperator<Row> source = env.addSource(new FileUrlClickRowDataSource())
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

        tableNameSource.addSink(new HBaseCustomSinkFunction(
                "tableName",
                2,
                columnFamilyMap,
                (RowTypeInfo) TABLE_USER_CLICK_TYPEINFO,
                2097152,
                5,
                60));

        env.execute("HBase custom sink training");
    }

}
