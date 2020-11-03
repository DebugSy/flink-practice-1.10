package com.flink.demo.cases.case15;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

public class HiveStreamingSinkTraining {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /*
        * checkpoint
        * */
        env.enableCheckpointing(1000 * 5);

        String name = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir = "E:\\GitHub\\test\\flink-practice-1.11\\src\\main\\resources\\hadoop";
        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tEnv.registerCatalog(name, hiveCatalog);
        tEnv.useCatalog(name);

        TableResult tableResult = tEnv.executeSql("show tables");
        CloseableIterator<Row> collect = tableResult.collect();
        while (collect.hasNext()) {
            Row next = collect.next();
            System.err.println(next);
        }

        SingleOutputStreamOperator<Row> streamSource = env.addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO);
        tEnv.createTemporaryView("url_click", streamSource);

        /*
        tEnv
                .connect(new FileSystem().path("/tmp/shiy/flink/sql_sink"))
                .withSchema(new Schema()
                        .field("userId", "int")
                        .field("username", "string")
                        .field("url", "string")
                        .field("clickTime", "timestamp")
                        .field("rank", "int")
                        .field("uuid", "string")
                        .field("data_col", "string")
                        .field("time_col", "string"))
                .withFormat(new Csv())
                .inAppendMode()
                .createTemporaryTable("sink_table");
         */

        tEnv.executeSql("insert into shiy_flink_sink select * from url_click");

        env.execute("flink table training");
    }

}
