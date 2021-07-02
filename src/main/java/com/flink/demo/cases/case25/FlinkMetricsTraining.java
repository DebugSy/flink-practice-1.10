package com.flink.demo.cases.case25;

import com.flink.demo.cases.common.datasource.AllDataTypeDataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

public class FlinkMetricsTraining {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //prometheus
        configuration.setString("metrics.reporter.promgateway.class", "org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter");
        configuration.setString("metrics.reporter.promgateway.host", "192.168.1.17");
        configuration.setString("metrics.reporter.promgateway.port", "9091");
        configuration.setString("metrics.reporter.promgateway.jobName", "flink_metrics_");
        configuration.setString("metrics.reporter.promgateway.randomJobNameSuffix", "true");
        configuration.setString("metrics.reporter.promgateway.deleteOnShutdown", "true");

        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, configuration);
        env.setParallelism(1);
        env.enableCheckpointing(10000);


        RowTypeInfo rowTypeInfo = (RowTypeInfo) AllDataTypeDataSource.allDataTypeInfo;
        DataStream<Row> allDataTypeStream = env.addSource(new AllDataTypeDataSource())
                .returns(rowTypeInfo)
                .setParallelism(2)
                .name("all data type source");

        SingleOutputStreamOperator<Row> result = allDataTypeStream
                .map(new CounterTrainFunction())
                .name("Counter Training")
                .returns(rowTypeInfo)
                .setParallelism(1)

                .map(new GaugeTrainFunction())
                .name("Gauge Training")
                .returns(rowTypeInfo)
                .setParallelism(2)

                .map(new MeterTrainFunction())
                .name("Meter Training")
                .returns(rowTypeInfo)
                .setParallelism(1)

                .map(new HistogramTrainFunction())
                .name("Histogram Training")
                .returns(rowTypeInfo)
                .setParallelism(2);

        env.execute("Flink Metrics training");
    }

}
