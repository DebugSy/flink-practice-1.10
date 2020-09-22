package com.flink.demo.cases.case10.finalState;

import com.flink.demo.cases.common.utils.ClassUtil;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.List;

/**
 * @author P0007
 * @version 1.0
 * @date 2020/4/21 18:57
 */
@Slf4j
@RequiredArgsConstructor
public class FinalStateAggregate extends ProcessWindowFunction<Row, Row, Tuple, TimeWindow> {

    @NonNull
    private RowTypeInfo rowTypeInfo;

    @NonNull
    private int finalStateColumnIndex;

    @NonNull
    private List<String> finalStateValues;

    private transient long mergedCnt = 0L;

    private transient long processedCnt = 0L;

    private transient long finalStateCnt = 0L;

    @Override
    public void open(Configuration parameters) throws Exception {
        //metrics
        Gauge<Long> mergedCntGauge = getRuntimeContext()
                .getMetricGroup()
                .addGroup("custom_group")
                .gauge("mergedCnt", () -> mergedCnt);

        Gauge<Long> processedCntGauge = getRuntimeContext()
                .getMetricGroup()
                .addGroup("custom_group")
                .gauge("processedCnt", () -> processedCnt);

        Gauge<Long> finalStatusCntGauge = getRuntimeContext()
                .getMetricGroup()
                .addGroup("custom_group")
                .gauge("finalStatusCnt", () -> finalStateCnt);
    }

    @Override
    public void process(Tuple tuple, Context context, Iterable<Row> elements, Collector<Row> out) throws Exception {
        /*
            窗口数据未按watermark排序
            1. 如果有最终状态，则合并的基准数据是最终状态的数据
            2. 如果没有最终状态，则合并的基准数据是最后一条数据
         */
        Iterator<Row> finalStateIter = elements.iterator();
        Row finalStateRow = null;
        while (finalStateIter.hasNext()) {
            Row element = finalStateIter.next();
            Object finalStateColValue = element.getField(finalStateColumnIndex);
            if (finalStateColValue != null && finalStateValues.contains(finalStateColValue)) {
                finalStateRow = element;
                finalStateCnt++;
                break;
            } else {
                finalStateRow = element;
            }
        }

        /*
            以finalStateRow为基准数据合并
         */
        Iterator<Row> iterator = elements.iterator();
        if (iterator.hasNext()) {
            Row mergedRow = finalStateRow;
            while (iterator.hasNext()) {
                Row row = iterator.next();
                log.debug("Merging row :\n{}\n{}", row, mergedRow);
                if (row.getArity() != mergedRow.getArity()) {
                    throw new RuntimeException("Row arity not equal");
                } else {
                    for (int i = 0; i < row.getArity(); i++) {
                        Object mergedRowFieldV = mergedRow.getField(i);
                        Object rowFieldV = row.getField(i);
                        String type = rowTypeInfo.getTypeAt(i).toString();
                        mergedRow.setField(i, ClassUtil.isEmpty(mergedRowFieldV, type) ? rowFieldV : mergedRowFieldV);
                        mergedCnt++;
                    }
                }
                processedCnt++;
            }
            log.debug("Merged result {}", mergedRow);
            out.collect(mergedRow);
        }
    }
}