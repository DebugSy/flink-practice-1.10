package com.flink.demo.cases.debug;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;

public class DebugMain {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        tEnv.registerFunction("TO_TIMESTAMP", new ToTimestamp());
        tEnv.registerFunction("TO_STRING", new ToString());
        tEnv.registerFunction("FROM_UNIXTIME", new FromUnixtime());

        DataStreamSource<String> source = env.fromCollection(Arrays.asList("1618479763321858"));
        tEnv.createTemporaryView("debug_source", source, "PROCEDURE_END_TIME");

//        Table table = tEnv.sqlQuery("select TO_STRING(to_timestamp((cast(PROCEDURE_END_TIME as bigint)/(1000*1000) - mod(cast(PROCEDURE_END_TIME as bigint)/(1000*1000), 300))*1000), 'yyyyMMddHHmm') as MINUTE_5 from debug_source");
        Table table = tEnv.sqlQuery("select TO_STRING(CURRENT_TIMESTAMP,'yyyyMMddHHmmss') as DATE_TIME from debug_source");

        DataStream<Row> sinkStream = tEnv.toAppendStream(table, Row.class);
        sinkStream.printToErr();



        env.execute("Debug Test");

    }

}
