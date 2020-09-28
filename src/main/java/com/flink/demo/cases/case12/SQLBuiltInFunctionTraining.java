package com.flink.demo.cases.case12;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

public class SQLBuiltInFunctionTraining {


    private static String sql =
            "SELECT rowKeyGen(rowKeyGen(username)) FROM clicks";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Row> clickStream = env.addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO);
        SingleOutputStreamOperator<Row> clickStreamAndWatermarks = clickStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row element) {
                return Timestamp.valueOf(element.getField(3).toString()).getTime();
            }
        });
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.createTemporaryView("clicks", clickStreamAndWatermarks, UrlClickRowDataSource.CLICK_FIELDS_WITH_ROWTIME);

        //udf
        tableEnv.registerFunction("rowKeyGen", new RowKeyGen());

        Table table = tableEnv.sqlQuery(sql);
        Table select = table.select("withColumns(1)");
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(select, Row.class);
        rowDataStream.printToErr();

        env.execute("Flink Stream Join Training");
    }

}
