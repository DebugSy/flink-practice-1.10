package com.flink.demo.cases.case16;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import ru.ivi.opensource.flinkclickhousesink.ClickHouseSink;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseClusterSettings;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkConst;

import java.util.Properties;

public class ClickHouseSinkTraining {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Row> source = env.addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO)
                .name("url click source");

        // ClickHouse cluster properties
        ExecutionConfig executionConfig = env.getConfig();
        Configuration configuration = new Configuration();
        configuration.setString(ClickHouseClusterSettings.CLICKHOUSE_HOSTS, "192.168.1.151:8123");
        configuration.setString(ClickHouseClusterSettings.CLICKHOUSE_USER, "default");
        configuration.setString(ClickHouseClusterSettings.CLICKHOUSE_PASSWORD, "");

        // sink common
        configuration.setLong(ClickHouseSinkConst.TIMEOUT_SEC, 10000);
        configuration.setString(ClickHouseSinkConst.FAILED_RECORDS_PATH, "/tmp/shiy/clickhouse");
        configuration.setLong(ClickHouseSinkConst.NUM_WRITERS, 1);
        configuration.setLong(ClickHouseSinkConst.NUM_RETRIES, 1);
        configuration.setLong(ClickHouseSinkConst.QUEUE_MAX_CAPACITY, 1);
        configuration.setBoolean(ClickHouseSinkConst.IGNORING_CLICKHOUSE_SENDING_EXCEPTION_ENABLED, true);

        executionConfig.setGlobalJobParameters(configuration);

        // create props for sink
        Properties props = new Properties();
        props.put(ClickHouseSinkConst.TARGET_TABLE_NAME, "shiy_flink_sink_test");
        props.put(ClickHouseSinkConst.MAX_BUFFER_SIZE, "1");

        RowTypeInfo rowTypeInfo = (RowTypeInfo)source.getType();

        SnowballCsvRowSerializer serializationSchema = new SnowballCsvRowSerializer.Builder(rowTypeInfo)
                .setEscapeCharacter('\\')
                .setNullLiteral("NULL")
                .setLineDelimiter("")
                .setFieldDelimiter(',')
                .setQuoteCharacter('\'')
                .build();
        StringBuilder strBuilder = new StringBuilder();
        SingleOutputStreamOperator<String> stringDataStream = source.map(row -> {
            String serialize = serializationSchema.serialize(row);
            strBuilder.append("(").append(serialize).append(")");
            String result = strBuilder.toString();
            System.err.println(result);
            strBuilder.setLength(0);
            return result;
        });

        stringDataStream.addSink(new ClickHouseSink(props)).name("sink");

        env.execute();
    }




}