package com.flink.demo.cases.case26;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.sink.filesystem.*;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Preconditions;

/**
 * StreamingFileSink的扩展,实现bucket关闭时,发出通知的功能
 *
 * @param <IN>       输入数据类型
 * @param <BucketId> BucketId类型
 */
public class StreamingFileSinkOperator<IN, BucketId> extends AbstractStreamOperator<BucketMessage>
        implements OneInputStreamOperator<IN, BucketMessage> {

    // ------------------------ configuration fields --------------------------

    private final long bucketCheckInterval;

    private BucketLifeCycleListener bucketLifeCycleListener;

    private final StreamingFileSink.BucketsBuilder<IN, BucketId, ? extends StreamingFileSink.BucketsBuilder<IN, BucketId, ?>> bucketsBuilder;

    // --------------------------- runtime fields -----------------------------

    private transient StreamingFileSinkHelper<IN> helper;

    /**
     * We listen to this ourselves because we don't have an {@link InternalTimerService}.
     */
    private long currentWatermark = Long.MIN_VALUE;


    /**
     * Creates a new {@code StreamingFileSink} that writes files to the given base directory
     * with the give buckets properties.
     */
    public StreamingFileSinkOperator(
            StreamingFileSink.BucketsBuilder<IN, BucketId, ? extends StreamingFileSink.BucketsBuilder<IN, BucketId, ?>> bucketsBuilder,
            long bucketCheckInterval) {

        Preconditions.checkArgument(bucketCheckInterval > 0L);

        this.bucketsBuilder = Preconditions.checkNotNull(bucketsBuilder);
        this.bucketCheckInterval = bucketCheckInterval;
    }

    /**
     * Creates a new {@code StreamingFileSink} that writes files to the given base directory
     * with the give buckets properties.
     */
    public StreamingFileSinkOperator(
            StreamingFileSink.BucketsBuilder<IN, BucketId, ? extends StreamingFileSink.BucketsBuilder<IN, BucketId, ?>> bucketsBuilder,
            long bucketCheckInterval,
            BucketLifeCycleListener bucketLifeCycleListener) {

        Preconditions.checkArgument(bucketCheckInterval > 0L);

        this.bucketsBuilder = Preconditions.checkNotNull(bucketsBuilder);
        this.bucketCheckInterval = bucketCheckInterval;
        this.bucketLifeCycleListener = bucketLifeCycleListener;
    }

    // --------------------------- Sink Methods -----------------------------

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        Buckets<IN, BucketId> buckets = bucketsBuilder.createBuckets(getRuntimeContext().getIndexOfThisSubtask());


        // Set listener before the initialization of Buckets.
        buckets.setBucketLifeCycleListener(new BucketLifeCycleListener<IN, BucketId>() {

            @Override
            public void bucketCreated(Bucket<IN, BucketId> bucket) {

            }

            @Override
            public void bucketInactive(Bucket<IN, BucketId> bucket) {
                BucketMessage bucketMessage = new BucketMessage(
                        bucket.getBucketId(),
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getNumberOfParallelSubtasks(),
                        bucket.getRecords(),
                        bucket.getBucketPath());
                output.collect(new StreamRecord<>(bucketMessage));
            }
        });
        buckets.setNumberOfTasks(getRuntimeContext().getNumberOfParallelSubtasks());
        this.helper = new StreamingFileSinkHelper<>(
                buckets,
                context.isRestored(),
                context.getOperatorStateStore(),
                getRuntimeContext().getProcessingTimeService(),
                bucketCheckInterval);
        currentWatermark = Long.MIN_VALUE;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        this.helper.commitUpToCheckpoint(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        Preconditions.checkState(helper != null, "sink has not been initialized");
        this.helper.snapshotState(context.getCheckpointId());
    }

    @Override
    public void close() throws Exception {
        if (this.helper != null) {
            this.helper.close();
        }
    }


    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        ProcessingTimeService processingTimeService = getProcessingTimeService();
        long currentProcessingTime = processingTimeService.getCurrentProcessingTime();
        boolean hasTimestamp = element.hasTimestamp();
        Long timestamp = hasTimestamp ? element.getTimestamp() : null;
        this.helper.onElement(
                element.getValue(),
                currentProcessingTime,
                timestamp,
                currentWatermark);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);
        this.currentWatermark = mark.getTimestamp();
    }

}
