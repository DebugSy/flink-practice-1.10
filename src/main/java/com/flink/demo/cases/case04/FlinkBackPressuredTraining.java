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
        configuration.setString("metrics.reporter.promgateway.host", "192.168.1.17");
        configuration.setString("metrics.reporter.promgateway.port", "9091");
        configuration.setString("metrics.reporter.promgateway.jobName", "flink_metrics_");
        configuration.setString("metrics.reporter.promgateway.randomJobNameSuffix", "true");
        configuration.setString("metrics.reporter.promgateway.deleteOnShutdown", "true");


        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, configuration);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.enableCheckpointing(1000 * 1, CheckpointingMode.EXACTLY_ONCE);

        SingleOutputStreamOperator<Row> sourceStream = env
                .addSource(new UrlClickRowDataSource())
                .setParallelism(16)
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO)
                .name("source");

        sourceStream.printToErr();

        env.execute("Flink Back Pressured Training");

    }

}
