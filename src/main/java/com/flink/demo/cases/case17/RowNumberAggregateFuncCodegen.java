package com.flink.demo.cases.case17;

import com.flink.demo.cases.common.datasource.OutOfOrderDataSource;
import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.List;

/**
 * Created by P0007 on 2019/9/29.
 */
public class RowNumberAggregateFuncCodegen {

//    private static String hopWindowSql = "select userId,count(url)," +
//            "CONCAT_AGG('/', username) as cname," +
//            "TUMBLE_START(clickTime, INTERVAL '5' SECOND) as window_start, " +
//            "TUMBLE_END(clickTime, INTERVAL '5' SECOND) as window_end " +
//            " from clicks group by userId, TUMBLE(clickTime, INTERVAL '5' SECOND)";

    private static String hopWindowSql = "select * from clicks";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Row> streamSource = env.addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO);

        SingleOutputStreamOperator<Row> streamSourceWithWatermarks =
                streamSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
                    @Override
                    public long extractAscendingTimestamp(Row element) {
                        return Timestamp.valueOf(element.getField(3).toString()).getTime();
                    }
                });

        SingleOutputStreamOperator<Row> map = streamSourceWithWatermarks.process(new ProcessFunction<Row, Row>() {
            @Override
            public void processElement(Row value, Context ctx, Collector<Row> out) throws Exception {
                out.collect(value);
            }
        }).returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO);

        tEnv.createTemporaryView("clicks", map, UrlClickRowDataSource.CLICK_FIELDS_WITH_ROWTIME);
        tEnv.registerFunction("CONCAT_AGG", new ConcatAggFunction());

        Table table = tEnv.sqlQuery(hopWindowSql);
        DataStream<Row> sinkStream = tEnv.toAppendStream(table, UrlClickRowDataSource.USER_CLICK_TYPEINFO);
        sinkStream.printToErr();


        env.execute("flink job demo");
    }

}
