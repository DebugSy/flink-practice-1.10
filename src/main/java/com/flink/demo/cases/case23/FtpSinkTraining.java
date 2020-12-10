package com.flink.demo.cases.case23;

import com.flink.demo.cases.case23.bucketing.BucketingSink;
import com.flink.demo.cases.case23.bucketing.DateTimeBucketer;
import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

public class FtpSinkTraining {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        env.enableCheckpointing(1000 * 5);
        env.setStateBackend(new FsStateBackend("file:///tmp/shiy/flink/checkpoint/"));
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        SingleOutputStreamOperator<Row> urlClickSource = env.addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO)
                .setParallelism(2);

        BucketingSink bucketingSink = new BucketingSink("/testftp/ftp-sink");
//        BucketingSink bucketingSink = new BucketingSink("/home/sftpuser/sftp-sink");
        bucketingSink.setBatchRolloverInterval(1000 * 5);
//        bucketingSink.setInactiveBucketThreshold(60);
        bucketingSink.setInactiveBucketCheckInterval(1000 * 60);
        DateTimeBucketer dateTimeBucketer = new DateTimeBucketer("yyyy-MM-dd-HHmm");
        bucketingSink.setBucketer(dateTimeBucketer);
        bucketingSink.setFTPConfig(new Configuration());
        urlClickSource.addSink(bucketingSink);

        env.execute("FTP Sink Training");
    }

}
