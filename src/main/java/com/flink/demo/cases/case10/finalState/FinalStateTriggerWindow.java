package com.flink.demo.cases.case10.finalState;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @author P0007
 * @version 1.0
 * @date 2020/4/21 14:16
 * <p>
 * 最终状态触发窗口计算
 */
@Slf4j
@AllArgsConstructor
public class FinalStateTriggerWindow {

    private DataStream<Row> inputStream;

    private RowTypeInfo rowTypeInfo;

    private String keyCol;

    public DataStream<Row> process(String finalStateColumn,
                                   List<String> finalStateValues,
                                   long waitTime,
                                   long lateness) {

        int keyColIndex = checkAndGetKeyColIndex(keyCol);
        int finalStateColumnIndex = checkAndGetKeyColIndex(finalStateColumn);
        KeyedStream<Row, Tuple> keyedStream = inputStream.keyBy(keyColIndex);
        SingleOutputStreamOperator<Row> finalStateResult = keyedStream.timeWindow(Time.seconds(waitTime))
                .trigger(new FinalStateTrigger(finalStateColumnIndex, finalStateValues))
                .allowedLateness(Time.seconds(lateness))
                .process(new FinalStateAggregate(rowTypeInfo, finalStateColumnIndex, finalStateValues))
                .returns(rowTypeInfo)
                .name("FinalStateTriggerWindow")
                .uid("FinalStateTriggerWindow");
        return finalStateResult;
    }

    private int checkAndGetKeyColIndex(String keyCol) {
        int keyColIndex = rowTypeInfo.getFieldIndex(keyCol);
        if (keyColIndex == -1) {
            throw new RuntimeException(keyCol + " not exist in " + rowTypeInfo);
        }
        return keyColIndex;
    }


}
