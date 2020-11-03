package com.flink.demo.cases.case17;

import com.flink.demo.cases.common.datasource.OutOfOrderDataSource;
import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
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

import java.sql.Timestamp;
import java.util.List;

/**
 * Created by P0007 on 2019/9/29.
 */
public class RowNumberAggregateFuncCodegen {

    private static String hopWindowSql = "select \n" +
            "\tuserId,username,url,clickTime,rank_num,uuid,data_col,time_col,row_number_rank\n" +
            "from (\n" +
            "\tselect \n" +
            "\t\tuserId,username,url,clickTime,rank_num,uuid,data_col,time_col,\n" +
            "\t\tROW_NUMBER() OVER(PARTITION BY userId ORDER BY rank_num) as row_number_rank\n" +
            "\tfrom \n" +
            "\t\tclicks\n" +
            ")\n" +
            "where row_number_rank <= 5";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings );
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Row> streamSource = env.addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO);

        SingleOutputStreamOperator<Row> streamSourceWithWatermarks = streamSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row element) {
                return Timestamp.valueOf(element.getField(3).toString()).getTime();
            }
        });

        tEnv.createTemporaryView("clicks", streamSourceWithWatermarks, UrlClickRowDataSource.CLICK_FIELDS_WITH_ROWTIME);

        Table table = tEnv.sqlQuery(hopWindowSql);
        DataStream<Tuple2<Boolean, Row>> sinkStream = tEnv.toRetractStream(table, Row.class);
        sinkStream.printToErr();

        env.execute("flink job demo");
    }

}
