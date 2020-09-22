package com.flink.demo.cases.case10.count;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * A {@link Trigger} that fires at some point after a specified number of
 * input elements have arrived.
 */
public class ElementTriggers {

	/**
	 * This class should never be instantiated.
	 */
	private ElementTriggers() {}

	/**
	 * Creates a new trigger that triggers on receiving of every element.
	 */
	public static <T, W extends Window> EveryElement<T, W> every() {
		return new EveryElement<>();
	}

	/**
	 * Creates a trigger that fires when the pane contains at lease {@code countElems} elements.
	 */
	public static <T, W extends Window> CountElement<T, W> count(long countElems) {
		return new CountElement<>(countElems);
	}

	/**
	 * A {@link Trigger} that triggers on every element.
	 * @param <W> type of window
	 */
	public static final class EveryElement<T, W extends Window> extends Trigger<T, W> {

		private static final long serialVersionUID = 3942805366646141029L;


		@Override
		public TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) throws Exception {
			return TriggerResult.FIRE;
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
		public void clear(W window, TriggerContext ctx) throws Exception {

		}
	}

	/**
	 * A {@link Trigger} that fires at some point after a specified number of
	 * input elements have arrived.
	 */
	public static final class CountElement<T, W extends Window> extends Trigger<T, W> {

		private static final long serialVersionUID = -3823782971498746808L;

		private final long countElems;
		private final ReducingStateDescriptor<Long> countStateDesc;
		private transient TriggerContext ctx;

		CountElement(long countElems) {
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
