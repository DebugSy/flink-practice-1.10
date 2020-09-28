package com.flink.demo.cases.case13;

import com.flink.demo.cases.case10.finalState.FinalStateTriggerWindow;
import com.flink.demo.cases.common.datasource.UrlClickCRowDataSource;
import com.flink.demo.cases.common.datasource.UrlClickFinalStateRowDataSource;
import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import com.flink.demo.cases.common.utils.ClassUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Properties;

/**
 *
 * 自定义trigger实现最终状态触发窗口计算
 *
 */
@Slf4j
public class FlinkKafkaSourceTraining {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final RowTypeInfo rowTypeInfo = (RowTypeInfo) UrlClickRowDataSource.USER_CLICK_TYPEINFO;

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.1.82:9094");
        props.setProperty("group.id", "idea_local_test");
        FlinkKafkaConsumer010<String> kafkaConsumer010 =
                new FlinkKafkaConsumer010<>("shiy.flink.url.click", new SimpleStringSchema(), props);

//        kafkaConsumer010.setStartFromEarliest();
        kafkaConsumer010.setStartFromTimestamp(1600789914000L);


        SingleOutputStreamOperator<Row> urlClickSource = env.addSource(kafkaConsumer010)
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
                .returns(rowTypeInfo)
                .name("url click stream source");

        urlClickSource.printToErr();

        env.execute("Kafka Source Training");

    }

}
