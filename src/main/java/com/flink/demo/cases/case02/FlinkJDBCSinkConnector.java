package com.flink.demo.cases.case02;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

public class FlinkJDBCSinkConnector {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Row> source = env.addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO)
                .name("url click source");

        source.addSink(JdbcSink.sink(
                "insert into shiy_flink_step_sink_url_click values (?,?,?,?,?,?,?,?)",
                (ps, t) -> {
                    ps.setInt(1, Integer.parseInt(t.getField(0).toString()));
                    ps.setString(2, t.getField(1).toString());
                    ps.setString(3, t.getField(2).toString());
                    ps.setString(4, t.getField(3).toString());
                    ps.setInt(5, Integer.parseInt(t.getField(4).toString()));
                    ps.setString(6, t.getField(5).toString());
                    ps.setString(7, null);
                    ps.setString(8, t.getField(7).toString());
                },
                JdbcExecutionOptions.builder().withBatchSize(10).withMaxRetries(3).withBatchIntervalMs(60000).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://192.168.1.83:8123/test")
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .withUsername("default")
                        .withPassword("123456")
                        .build())
        ).name("sink");

        env.execute();
    }

}
