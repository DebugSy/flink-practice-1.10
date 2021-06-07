package com.flink.demo.cases.case12;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

public class SQLBuiltInFunctionTraining {


    private static String sql =
            "SELECT username,T.p FROM clicks, LATERAL TABLE(EXPLODE(username, '')) as T(p)";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Row> clickStream = env.addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO);
        SingleOutputStreamOperator<Row> clickStreamAndWatermarks = clickStream
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
                    @Override
                    public long extractAscendingTimestamp(Row element) {
                        return Timestamp.valueOf(element.getField(3).toString()).getTime();
                    }
                });
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.createTemporaryView("clicks", clickStreamAndWatermarks, UrlClickRowDataSource.CLICK_FIELDS_WITH_ROWTIME);

        //udf
        tableEnv.registerFunction("EXPLODE", new ExplodeUDTF());

        Table table = tableEnv.sqlQuery(sql);
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);
        rowDataStream.printToErr();

        env.execute("Flink Stream Join Training");
    }

}
