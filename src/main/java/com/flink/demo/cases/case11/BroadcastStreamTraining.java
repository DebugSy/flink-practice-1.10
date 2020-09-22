package com.flink.demo.cases.case11;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

/**
 * 利用Broadcast Stream实现控制作用输出到HBase的表名
 */
public class BroadcastStreamTraining {

    public static TypeInformation TABLE_USER_CLICK_TYPEINFO = Types.ROW(
            new String[]{"namespace", "tableName", "userId", "username", "url", "clickTime", "data_col", "time_col"},
            new TypeInformation[]{
                    Types.STRING(),
                    Types.STRING(),
                    Types.INT(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.SQL_TIMESTAMP(),
                    Types.STRING(),
                    Types.STRING()
            });

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        RowTypeInfo rowTypeInfo = (RowTypeInfo) UrlClickRowDataSource.USER_CLICK_TYPEINFO;

        SingleOutputStreamOperator<Row> source = env.addSource(new UrlClickRowDataSource())
                .returns(rowTypeInfo)
                .name("url click source");

        final MapStateDescriptor<String, String> configDesc = new MapStateDescriptor<>(
                "broadcast",
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO);
        BroadcastStream<Row> broadcastStream = env.addSource(new MinuteBroadcastSource())
                .returns(MinuteBroadcastSource.rowTypeInfo)
                .setParallelism(1)
                .broadcast(configDesc);

        SingleOutputStreamOperator<Row> resultStream =
                source.keyBy(1)
                        .connect(broadcastStream)
                        .process(new MyBroadcastProcessFunction())
                        .returns(TABLE_USER_CLICK_TYPEINFO);

        Map<String, String> columnFamilyMap = new HashMap<>();
        columnFamilyMap.put("userId", "cf1");
        columnFamilyMap.put("url", "cf1");
        columnFamilyMap.put("clickTime", "cf1");
        columnFamilyMap.put("data_col", "cf1");
        columnFamilyMap.put("time_col", "cf1");

        resultStream.addSink(new HBaseSinkFunction(3,
                columnFamilyMap,
                (RowTypeInfo) TABLE_USER_CLICK_TYPEINFO,
                2097152,
                1));

        env.execute("broadcast stream training");
    }

}
