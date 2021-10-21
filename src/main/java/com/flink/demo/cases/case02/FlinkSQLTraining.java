package com.flink.demo.cases.case02;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

public class FlinkSQLTraining {

    private static final String sql = "select userId,listagg(username) " +
            "from clicks " +
            "group by " +
            "userId,tumble(clickTime, INTERVAL '5' SECOND)";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        env.setParallelism(1);
        env.enableCheckpointing(1000 * 10);

        DataStream<Row> source = env.addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO)
                .name("url click source");

        SingleOutputStreamOperator<Row> watermarkStream = source.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forMonotonousTimestamps()
                        .withTimestampAssigner((element, timestamp) ->
                                ((Timestamp) element.getField(3)).getTime()));

        tableEnv.createTemporaryView("clicks", watermarkStream, UrlClickRowDataSource.CLICK_FIELDS_WITH_ROWTIME);
        Table table = tableEnv.sqlQuery(sql);
        DataStream<Row> sinkStream = tableEnv.toAppendStream(table, Row.class);
        sinkStream.printToErr();

        env.execute("Flink SQL Training");
    }

}
