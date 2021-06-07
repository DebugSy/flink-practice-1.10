package com.flink.demo.cases.case02;

import com.flink.demo.cases.common.datasource.AllDataTypeDataSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Types;

public class FlinkJDBCSinkConnector_Snowball {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator source = env.addSource(new AllDataTypeDataSource())
                .returns(AllDataTypeDataSource.allDataTypeInfo)
                .name("url click source");

        JDBCOutputFormat format = JDBCOutputFormat.buildJDBCOutputFormat()
                .setUsername("default")
                .setPassword("123456")
                .setDBUrl("jdbc:snowball://192.168.1.83:8123/test")
                .setQuery("insert into shiy_flink_step_sink_all_type_test values (?,?,?,?,?,?,?,?,?,?,?,?,?)")
                .setDrivername("com.inforefiner.snowball.SnowballDriver")
                .setBatchInterval(1)
                .setSqlTypes(new int[]{
                        Types.INTEGER,
                        Types.VARCHAR,
                        Types.BOOLEAN,
                        Types.TINYINT,
                        Types.TIMESTAMP,
                        Types.DATE,
                        Types.DECIMAL,
                        Types.DOUBLE,
                        Types.REAL,
                        Types.BIGINT,
                        Types.SMALLINT,
                        Types.BINARY,
                        Types.VARCHAR})
                .finish();


        source.addSink(new JDBCSinkFunction(format)).name("sink");

        env.execute();
    }

}
