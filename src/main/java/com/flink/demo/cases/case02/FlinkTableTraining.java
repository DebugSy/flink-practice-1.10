package com.flink.demo.cases.case02;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.functions.aggfunctions.ListAggFunction;
import org.apache.flink.table.planner.functions.sql.SqlListAggFunction;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

import static org.apache.flink.table.api.Expressions.*;

public class FlinkTableTraining {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        env.setParallelism(1);
        env.enableCheckpointing(1000 * 10);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        DataStream<Row> source = env.addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO)
                .name("url click source");

        SingleOutputStreamOperator<Row> watermarkStream = source.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forMonotonousTimestamps()
                        .withTimestampAssigner((element, timestamp) ->
                                ((Timestamp) element.getField(3)).getTime()));

        Table table = tableEnv.fromDataStream(watermarkStream,
                $("userId"),
                $("username"),
                $("url"),
                $("clickTime").rowtime(),
                $("user_rank"),
                $("uuid_col"),
                $("date_col"),
                $("time_col"));
        Table result = table
                .select($("userId"), callSql("UPPER(username)").as("name"));
        DataStream<Row> sinkStream = tableEnv.toAppendStream(result, Row.class);
        sinkStream.printToErr();

        env.execute("Flink SQL Training");
    }

}
