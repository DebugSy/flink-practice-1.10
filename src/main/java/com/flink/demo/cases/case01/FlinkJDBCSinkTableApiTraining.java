package com.flink.demo.cases.case01;

import com.flink.demo.cases.common.datasource.MockCDCRowDataSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.sql.Timestamp;

/**
 * JDBC Table Api测试
 */
public class FlinkJDBCSinkTableApiTraining {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        env.setParallelism(1);
        env.enableCheckpointing(1000 * 10);

        DataStream<Row> source = env.addSource(new MockCDCRowDataSource())
                .returns(MockCDCRowDataSource.USER_CLICK_TYPEINFO)
                .name("url click source");

        Schema schema = Schema.newBuilder()
                .column("userId", DataTypes.INT())
                .column("username", DataTypes.STRING())
                .column("url", DataTypes.STRING())
                .column("clickTime", DataTypes.TIMESTAMP(3).bridgedTo(Timestamp.class))
                .column("user_rank", DataTypes.INT())
                .column("uuid_col", DataTypes.STRING())
                .column("date_col", DataTypes.STRING())
                .column("time_col", DataTypes.STRING())
                .build();

        Table sourceTable = tableEnv.fromChangelogStream(source, schema);
        sourceTable.printSchema();

        tableEnv.executeSql(
                "CREATE TABLE jdbc_SINK(\n"
                        + "  userId INT,\n"
                        + "  username STRING,\n"
                        + "  url STRING,\n"
                        + "  clickTime TIMESTAMP,\n"
                        + "  user_rank INT,\n"
                        + "  uuid_col STRING,\n"
                        + "  date_col STRING,\n"
                        + "  time_col STRING,\n"
                        + "   PRIMARY KEY (userId) NOT ENFORCED"
                        + ") WITH (\n"
                        + "  'connector' = 'jdbc',\n"
                        + "  'url' = 'jdbc:mysql://localhost:3306/test',\n"
                        + "  'table-name' = 'step_url_click_v3'\n"
                        + ")");
        sourceTable.executeInsert("jdbc_SINK");

        DataStream<Row> rowDataStream = tableEnv.toChangelogStream(sourceTable);
        rowDataStream.printToErr();

        env.execute();
    }

}
