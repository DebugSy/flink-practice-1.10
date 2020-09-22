package com.flink.demo.cases.case10.count;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class TimeTriggers {


    public static final class ProcessingTime<T, W extends Window> extends Trigger<T, W> {

        private static final long serialVersionUID = -3823782971498746808L;

        private final long countElems;
        private final ReducingStateDescriptor<Long> countStateDesc;
        private transient TriggerContext ctx;

        ProcessingTime(long countElems) {
            this.countElems = countElems;
            this.countStateDesc = new ReducingStateDescriptor<>(
                    "trigger-count-" + countElems, new Sum(), LongSerializer.INSTANCE);
        }

        @Override
        public TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) throws Exception {
            ReducingState<Long> count = ctx.getPartitionedState(countStateDesc);
            count.add(1L);
            if (count.get() >= countElems) {
                count.clear();
                return TriggerResult.FIRE;
            } else {
                return TriggerResult.CONTINUE;
            }
        }

        @Override
        public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public boolean canMerge() {
            return true;
        }

        @Override
        public void onMerge(W window, OnMergeContext mergeContext) throws Exception {
            mergeContext.mergePartitionedState(countStateDesc);
        }

        @Override
        public void clear(W window, TriggerContext ctx) throws Exception {

        }

        @Override
        public String toString() {
            return "Element.count(" + countElems + ")";
        }

        private static class Sum implements ReduceFunction<Long> {
            private static final long serialVersionUID = 1L;

            @Override
            public Long reduce(Long value1, Long value2) throws Exception {
                return value1 + value2;
            }

        }
    }

}
