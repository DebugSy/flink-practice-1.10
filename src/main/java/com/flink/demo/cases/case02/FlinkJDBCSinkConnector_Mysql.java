package com.flink.demo.cases.case02;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import com.flink.demo.cases.common.utils.ClassUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.types.Row;

import java.sql.Types;
import java.util.Properties;

@Slf4j
public class FlinkJDBCSinkConnector_Mysql {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000 * 10);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));
        env.setParallelism(1);

        RowTypeInfo rowTypeInfo = (RowTypeInfo) UrlClickRowDataSource.USER_CLICK_TYPEINFO;

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.1.82:9094");
        props.setProperty("group.id", "idea_local_test");
        FlinkKafkaConsumer010<String> kafkaConsumer010 =
                new FlinkKafkaConsumer010<>("shiy.flink.url.click", new SimpleStringSchema(), props);

        DataStream<Row> source = env.addSource(kafkaConsumer010)
                .map(new RichMapFunction<String, Row>() {
                    int counter = 0;
                    Row row = new Row(rowTypeInfo.getArity());
                    @Override
                    public Row map(String value) throws Exception {
                        String[] split = value.split(",", -1);
                        for (int i = 0; i < split.length; i++) {
                            String valueStr = split[i];
                            Object convert = ClassUtil.convert(valueStr, rowTypeInfo.getTypeAt(i).toString());
                            row.setField(i, convert);
                        }
                        log.info("counter {}, consume message {}", counter++, row);
                        return row;
                    }
                })
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO)
                .name("url click stream source");

        source.printToErr();

        JDBCOutputFormat format = JDBCOutputFormat.buildJDBCOutputFormat()
                .setUsername("root")
                .setPassword("")
                .setDBUrl("jdbc:mysql://localhost:3306/test")
                .setQuery("insert into url_click values (?,?,?,?,?,?,?,?)")
                .setDrivername("com.mysql.jdbc.Driver")
                .setBatchInterval(10)
                .setSqlTypes(new int[]{Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP, Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR})
                .finish();


        source.addSink(new JDBCSinkFunction(format)).name("sink");

        env.execute();
    }

}
