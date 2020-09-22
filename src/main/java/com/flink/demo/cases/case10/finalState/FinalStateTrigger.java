package com.flink.demo.cases.case10.finalState;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @author P0007
 * @version 1.0
 * @date 2020/4/21 18:56
 */
@Slf4j
public class FinalStateTrigger extends Trigger<Row, TimeWindow> {

    private int finalStateColumnIndex;

    private List<String> finalStateValues;

    public FinalStateTrigger(int finalStateColumnIndex, List<String> finalStateValues) {
        this.finalStateColumnIndex = finalStateColumnIndex;
        this.finalStateValues = finalStateValues;
    }

    @Override
    public TriggerResult onElement(Row element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        TriggerResult triggerResult;
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            triggerResult = TriggerResult.FIRE;
        } else {
            Object finalStateColValue = element.getField(finalStateColumnIndex);
            if (finalStateColValue != null && finalStateValues.contains(finalStateColValue)) {
                triggerResult = TriggerResult.FIRE;
            } else {
                ctx.registerEventTimeTimer(window.maxTimestamp());
                triggerResult = TriggerResult.CONTINUE;
            }
        }
        log.debug("OnElement Event, element is {}, timestamp is {}, watermark is {}, window is {}, trigger result is {}",
                element, timestamp, ctx.getCurrentWatermark(), window, triggerResult);
        return triggerResult;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        log.debug("onProcessingTime Event, time is {}, window is {}", time, window);
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        log.debug("onEventTime Event, time is {}, watermark is {}, window max timestamp is {}, window is {}",
                time, ctx.getCurrentWatermark(), window.maxTimestamp(), window);
        return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        log.debug("OnClear Event, watermark is {}", ctx.getCurrentWatermark());
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }
}
