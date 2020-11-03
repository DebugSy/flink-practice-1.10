package com.flink.demo.cases.case12;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.Timestamp;

public class SQLBuiltInFunctionTraining {


    private static String sql =
            "SELECT * FROM clicks";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Row> clickStream = env.addSource(new UrlClickRowDataSource(), "dummy_source",
                TypeExtractor.getInputFormatTypes(new InputFormat<Row, InputSplit>() {
                    @Override
                    public void configure(Configuration parameters) {

                    }

                    @Override
                    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
                        return null;
                    }

                    @Override
                    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
                        return new InputSplit[0];
                    }

                    @Override
                    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
                        return null;
                    }

                    @Override
                    public void open(InputSplit split) throws IOException {

                    }

                    @Override
                    public boolean reachedEnd() throws IOException {
                        return false;
                    }

                    @Override
                    public Row nextRecord(Row reuse) throws IOException {
                        return null;
                    }

                    @Override
                    public void close() throws IOException {

                    }
                }));

        SingleOutputStreamOperator returns = clickStream.map(r -> r).returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO);


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.createTemporaryView("clicks", returns, UrlClickRowDataSource.CLICK_FIELDS_WITH_ROWTIME);

        //udf
        tableEnv.registerFunction("rowKeyGen", new RowKeyGen());

        Table table = tableEnv.sqlQuery(sql);
//        Table select = table.select("withColumns(1)");
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, GenericTypeInfo.of(Row.class));
        rowDataStream.printToErr();

        env.execute("Flink Stream Join Training");
    }

}
