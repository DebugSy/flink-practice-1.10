package com.flink.demo.cases.case02;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import com.flink.demo.cases.common.utils.ClassUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * 已经证明: flink jdbc当任务失败后，导致数据重复写入DB
 *
 */
@Slf4j
public class FlinkJDBCSinkConnector_KafkaSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000 * 60);
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
