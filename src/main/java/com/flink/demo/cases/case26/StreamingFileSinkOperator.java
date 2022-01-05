package com.flink.demo.cases.case26;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.sink.filesystem.*;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Preconditions;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * StreamingFileSink的扩展,实现bucket关闭时,发出通知的功能
 *
 * @param <IN>       输入数据类型
 * @param <BucketId> BucketId类型
 */
public class StreamingFileSinkOperator<IN, BucketId> extends AbstractStreamOperator<BucketEvent>
        implements OneInputStreamOperator<IN, BucketEvent>, ProcessingTimeCallback {

    // ------------------------ configuration fields --------------------------

    private final long bucketCheckInterval;

    private final long bucketTimeout;

    private final StreamingFileSink.BucketsBuilder<IN, BucketId, ? extends StreamingFileSink.BucketsBuilder<IN, BucketId, ?>> bucketsBuilder;

    // --------------------------- runtime fields -----------------------------

    private transient StreamingFileSinkHelper<IN> helper;

    private transient ProcessingTimeService procTimeService;

    private static final String format = "yyyy-MM-dd HH:mm:ss.SSS";

    /**
     * We listen to this ourselves because we don't have an {@link InternalTimerService}.
     */
    private long currentWatermark = Long.MIN_VALUE;

    private transient Map<BucketId, BucketEvent> inactiveBuckets;


    /**
     * Creates a new {@code StreamingFileSink} that writes files to the given base directory
     * with the give buckets properties.
     */
    public StreamingFileSinkOperator(
            StreamingFileSink.BucketsBuilder<IN, BucketId, ? extends StreamingFileSink.BucketsBuilder<IN, BucketId, ?>> bucketsBuilder,
            long bucketCheckInterval,
            long bucketTimeout) {

        Preconditions.checkArgument(bucketCheckInterval > 0L);

        this.bucketsBuilder = Preconditions.checkNotNull(bucketsBuilder);
        this.bucketCheckInterval = bucketCheckInterval;
        this.bucketTimeout = bucketTimeout;
    }

    // --------------------------- Sink Methods -----------------------------

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        Buckets<IN, BucketId> buckets = bucketsBuilder.createBuckets(getRuntimeContext().getIndexOfThisSubtask());
        procTimeService = getRuntimeContext().getProcessingTimeService();

        // Set listener before the initialization of Buckets.
        inactiveBuckets = new HashMap<>();
        buckets.setBucketLifeCycleListener(new BucketLifeCycleListener<IN, BucketId>() {

            @Override
            public void bucketCreated(Bucket<IN, BucketId> bucket) {
                BucketId bucketId = bucket.getBucketId();
                LOG.debug("Bucket {} is created", bucketId);
                long createTime = bucket.getCreateTime();
                inactiveBuckets.computeIfAbsent(bucketId, v -> {
                    long time = createTime + bucketTimeout;
                    LOG.debug("Bucket {} create time is {}", bucketId, formatDate(createTime, format));
                    LOG.debug("Register timer {} for bucket {}", formatDate(time, format), bucketId);
                    procTimeService.registerTimer(time, StreamingFileSinkOperator.this);
                    BucketEvent bucketEvent = new BucketEvent(
                            bucketId,
                            getRuntimeContext().getIndexOfThisSubtask(),
                            getRuntimeContext().getNumberOfParallelSubtasks(),
                            bucket.getRecords(),
                            bucket.getBucketPath().getPath(),
                            bucket.getCreateTime());
                    return bucketEvent;
                });
            }

            @Override
            public void bucketInactive(Bucket<IN, BucketId> bucket) {
                BucketId bucketId = bucket.getBucketId();
                LOG.debug("Bucket {} is already inactive. inactive buckets size {}", bucketId, inactiveBuckets.size());
                inactiveBuckets.compute(bucketId, (k, v) -> {
                    if (v == null) {
                        throw new RuntimeException("Bucket " + bucketId + " was not exist.");
                    } else {
                        v.records += bucket.getRecords();
                        return v;
                    }
                });
            }
        });
        this.helper = new StreamingFileSinkHelper<>(
                buckets,
                context.isRestored(),
                context.getOperatorStateStore(),
                procTimeService,
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

    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        LOG.debug("On processing time trigger at time {}", formatDate(timestamp, format));
        Iterator<BucketId> iterator = inactiveBuckets.keySet().iterator();
        while (iterator.hasNext()) {
            BucketId bucketId = iterator.next();
            BucketEvent bucketEvent = inactiveBuckets.get(bucketId);
            Long creationTime = bucketEvent.createTime;
            LOG.debug("Bucket {} create time {}, curr time {}", bucketId,
                    formatDate(creationTime, format),
                    formatDate(timestamp, format));
            if (creationTime + bucketTimeout <= timestamp) {
                LOG.debug("Emit bucket event {}", bucketEvent);
                output.collect(new StreamRecord<>(bucketEvent));
                iterator.remove();
            }
        }
    }

    private String formatDate(long time, String format) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(time),
                ZoneId.systemDefault());
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
        return formatter.format(localDateTime);
    }
}
