package com.flink.demo.cases.case04;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Iterator;

@Slf4j
public class FlinkBackPressuredTraining {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString("taskmanager.memory.process.size", "1MB");
        configuration.setString("taskmanager.memory.flink.size", "1MB");
        configuration.setString("taskmanager.memory.managed.size", "1MB");
        configuration.setString("taskmanager.memory.size", "1MB");


        //prometheus
        configuration.setString("metrics.reporter.promgateway.class", "org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter");
        configuration.setString("metrics.reporter.promgateway.host", "192.168.2.96");
        configuration.setString("metrics.reporter.promgateway.port", "9091");
        configuration.setString("metrics.reporter.promgateway.jobName", "flink_metrics_");
        configuration.setString("metrics.reporter.promgateway.randomJobNameSuffix", "true");
        configuration.setString("metrics.reporter.promgateway.deleteOnShutdown", "true");


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.enableCheckpointing(1000 * 1, CheckpointingMode.EXACTLY_ONCE);

        env.disableOperatorChaining();

        SingleOutputStreamOperator<Row> sourceStream = env
                .addSource(new UrlClickRowDataSource())
                .setParallelism(16)
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO)
                .name("source");


        SingleOutputStreamOperator<Row> preFilter = sourceStream.filter(new FilterFunction<Row>() {
            @Override
            public boolean filter(Row value) throws Exception {
                int userId = Integer.parseInt(value.getField(0).toString());
                return userId > 0;
            }
        }).setParallelism(2).name("pre-filter");

        SingleOutputStreamOperator<Row> group1Stream = preFilter.slotSharingGroup("group1");

        SingleOutputStreamOperator<Row> streamWithWatermarks = group1Stream
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
                    @Override
                    public long extractAscendingTimestamp(Row element) {
                        return Timestamp.valueOf(element.getField(3).toString()).getTime();
                    }
                }).setParallelism(1).name("assignWatermark");

        SingleOutputStreamOperator<Row> group2Stream = streamWithWatermarks.slotSharingGroup("group2");

        SingleOutputStreamOperator<Row> filterStream1 = group2Stream.filter(new FilterFunction<Row>() {
            @Override
            public boolean filter(Row value) throws Exception {
                int userId = Integer.parseInt(value.getField(0).toString());
                return userId > 0;
            }
        }).setParallelism(2).name("first-filter");

        DataStream<Row> group3Stream = filterStream1.slotSharingGroup("group3");

        SingleOutputStreamOperator<Row> filterStream2 = group3Stream.filter(new FilterFunction<Row>() {
            @Override
            public boolean filter(Row value) throws Exception {
                int userId = Integer.parseInt(value.getField(0).toString());
                return userId > 0;
            }
        }).name("second-filter");

        SingleOutputStreamOperator<Row> group4Stream = filterStream2.slotSharingGroup("group4");


        KeyedStream<Row, String> keyedStream = group4Stream
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        return value.getField(1).toString();
                    }
                });


        SingleOutputStreamOperator<Row> aggregateStream = keyedStream
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(
                        new AggregateFunction<Row, Long, Row>() {


                            @Override
                            public Long createAccumulator() {
                                Long acc = new Long(0L);
                                return acc;
                            }

                            @Override
                            public Long add(Row value, Long accumulator) {
                                try {
                                    Thread.sleep(1000 * 10);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                                accumulator = accumulator + 1;
                                log.info("accumulate {}, acc is {}", value, accumulator);
                                return accumulator;
                            }

                            @Override
                            public Row getResult(Long accumulator) {
                                Row row = new Row(1);
                                row.setField(0, accumulator);
                                log.info("acc is {}", accumulator);
                                return row;
                            }

                            @Override
                            public Long merge(Long a, Long b) {
                                log.info("merge a {}, b {}");
                                return a + b;
                            }
                        },
                        new WindowFunction<Row, Row, String, TimeWindow>() {
                            @Override
                            public void apply(String s, TimeWindow window, Iterable<Row> input, Collector<Row> out) throws Exception {
                                Iterator<Row> iterator = input.iterator();
                                while (iterator.hasNext()) {
                                    Row row = new Row(5);
                                    row.setField(0, s);
                                    Row inputRow = iterator.next();
                                    row.setField(1, inputRow.getField(0));
                                    row.setField(2, new Timestamp(window.getStart()));
                                    row.setField(3, new Timestamp(window.getEnd()));
                                    row.setField(4, new Timestamp(window.maxTimestamp()));
                                    out.collect(row);
                                }

                            }
                        })
                .setParallelism(1)
                .name("aggregate");

        aggregateStream.print();

        env.execute("Flink Back Pressured Training");

    }

}
