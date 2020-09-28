package com.flink.demo.cases.case01;

import com.flink.demo.cases.common.datasource.OutOfOrderDataSource;
import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.internal.StreamTableEnvironmentImpl;
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
public class FlinkTrainingAggregateFuncCodegen {

    private static String hopWindowSql = "select username, count(*) as cnt, " +
            "HOP_ROWTIME(rowtime, INTERVAL '5' SECOND, INTERVAL '10' SECOND) as window_rowtime, " +
            "HOP_START(rowtime, INTERVAL '5' SECOND, INTERVAL '10' SECOND) as window_start, " +
            "HOP_END(rowtime, INTERVAL '5' SECOND, INTERVAL '10' SECOND) as window_end " +
            "from clicks " +
            "group by username, " +
            "HOP(rowtime, INTERVAL '5' SECOND, INTERVAL '10' SECOND)";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Class<? extends StreamTableEnvironment> tEnvClass = tEnv.getClass();
//        Field planner = tEnvClass.getField("planner");

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

        tEnv.createTemporaryView("clicks", streamSourceWithWatermarks, OutOfOrderDataSource.CLICK_FIELDS);


        Planner planner = ((StreamTableEnvironmentImpl) tEnv).getPlanner();

        Parser parser = planner.getParser();
        List<Operation> operations = parser.parse(hopWindowSql);
        System.out.println(operations);


        for (Operation operation : operations) {
            if (operation instanceof QueryOperation) {
                QueryOperation queryOperation = (QueryOperation) operation;
                TableSchema tableSchema = queryOperation.getTableSchema();
                List<TableColumn> tableColumns = tableSchema.getTableColumns();
                for (TableColumn column : tableColumns) {
                    System.out.println(column.getName() + " " + column.getType() + " " + column.getExpr());
                }
            } else {
                throw new RuntimeException("Unexpected exception. This is a bug. Please consider filing an issue.");
            }
        }

        Table table = tEnv.sqlQuery(hopWindowSql);
        DataStream<Row> sinkStream = tEnv.toAppendStream(table, Row.class);
        sinkStream.printToErr();

        env.execute("flink job demo");
    }

}
