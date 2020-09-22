package com.flink.demo.cases.case03;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.annotation.Nullable;

public class FlinkJobListenerDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /*
        * 注册JobListener时，开启一个线程，定时去上报job的状态
        * */
        env.registerJobListener(new JobListener() {
            @Override
            public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
                HeartbeatReporter.start();
            }

            @Override
            public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {
                System.err.println("executed");
                HeartbeatReporter.shutdown();
            }
        });

        SingleOutputStreamOperator streamSource = env.addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO)
                .name("url click stream source");
        streamSource.printToErr();

        env.execute();
    }

}
