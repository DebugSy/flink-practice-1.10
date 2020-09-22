package com.flink.demo.cases.case10.finalState;

import com.flink.demo.cases.common.datasource.UrlClickFinalStateRowDataSource;
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
public class FlinkWindowTriggerTraining_FinalState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final RowTypeInfo rowTypeInfo = (RowTypeInfo) UrlClickFinalStateRowDataSource.USER_CLICK_TYPEINFO;

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.1.82:9094");
        props.setProperty("group.id", "idea_local_test");
        FlinkKafkaConsumer010<String> kafkaConsumer010 =
                new FlinkKafkaConsumer010<>("shiy.flink.flinal.state.merge", new SimpleStringSchema(), props);
        SingleOutputStreamOperator<Row> urlClickSource = env.addSource(kafkaConsumer010)
                .map(new RichMapFunction<String, Row>() {

                    Row row = new Row(rowTypeInfo.getArity());

                    @Override
                    public Row map(String value) throws Exception {
                        String[] split = value.split(",", -1);
                        for (int i = 0; i < split.length; i++) {
                            String valueStr = split[i];
                            Object convert = ClassUtil.convert(valueStr, rowTypeInfo.getTypeAt(i).toString());
                            row.setField(i, convert);
                        }
                        log.info("consume message {}", row);
                        return row;
                    }
                })
                .returns(rowTypeInfo)
                .name("url click stream source");

        SingleOutputStreamOperator<Row> streamOperator = urlClickSource.assignTimestampsAndWatermarks(
                new AssignerWithPeriodicWatermarks<Row>() {

            private long currentTimestamp = Long.MIN_VALUE;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                long watermark = currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp;
                log.debug("current watermark is {}", watermark);
                return new Watermark(watermark);
            }

            @Override
            public long extractTimestamp(Row element, long previousElementTimestamp) {
                long newTimestamp = 0;
                newTimestamp = Timestamp.valueOf(element.getField(3).toString()).getTime();
                if (newTimestamp > this.currentTimestamp) {
                    this.currentTimestamp = newTimestamp;
                }
                log.debug("extractTimestamp {}", this.currentTimestamp);
                return this.currentTimestamp;
            }
        });

        FinalStateTriggerWindow finalStateTriggerWindow = new FinalStateTriggerWindow(
                streamOperator, rowTypeInfo, "username");
        DataStream<Row> mergedStream = finalStateTriggerWindow.process("final_state",
                Arrays.asList("FinalState1", "FinalState2", "FinalState3"),
                10,
                5);
        mergedStream.printToErr();


        env.execute("Incremental Window Aggregation with ReduceFunction");

    }

}
