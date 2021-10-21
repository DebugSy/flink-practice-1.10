package com.flink.demo.cases.case01;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import com.mysql.jdbc.jdbc2.optional.MysqlXADataSource;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

/**
 * JDBC Sink一致性语义
 */
public class FlinkJDBCSinkTraining {

    private static String jdbcUrl = "jdbc:mysql://192.168.1.17:3306/test";
    private static String username = "root";
    private static String password = "123456";
    private static String sql = "insert into " +
            "url_click(userId,username,url,clickTime,rank_num,uuid_col,date_col,time_col) " +
            "values(?,?,?,?,?,?,?,?)";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000 * 10);

        DataStream<Row> source = env.addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO)
                .name("url click source");

        source.addSink(JdbcSink.exactlyOnceSink(
                sql,
                ((ps, row) -> {
                    ps.setInt(1, (Integer) row.getField(0));
                    ps.setString(2, (String) row.getField(1));
                    ps.setString(3, (String) row.getField(2));
                    ps.setTimestamp(4, (Timestamp) row.getField(3));
                    ps.setInt(5, (Integer) row.getField(4));
                    ps.setString(6, (String) row.getField(5));
                    ps.setString(7, (String) row.getField(6));
                    ps.setString(8, (String) row.getField(7));
                }),
                JdbcExecutionOptions.builder()
                        .withBatchSize(10)
                        .build(),
                JdbcExactlyOnceOptions.defaults(),
                () -> {
                    MysqlXADataSource mysqlXADataSource = new MysqlXADataSource();
                    mysqlXADataSource.setUrl(jdbcUrl);
                    mysqlXADataSource.setUser(username);
                    mysqlXADataSource.setPassword(password);
                    return mysqlXADataSource;
                }
        ));

        env.execute();
    }

}
