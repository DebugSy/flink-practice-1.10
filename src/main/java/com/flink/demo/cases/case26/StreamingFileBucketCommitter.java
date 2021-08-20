package com.flink.demo.cases.case26;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.*;

/**
 * Bucket状态监听
 * <p>
 * 收集{@link StreamingFileSinkOperator}所有并行Task的bucket关闭消息并统计bucket内数据信息(数据记录数,文件大小)后通知注册的监听器
 *
 * @param <BucketId>
 */
@Slf4j
public class StreamingFileBucketCommitter<BucketId> extends AbstractStreamOperator<Void>
        implements OneInputStreamOperator<BucketEvent<BucketId>, Void> {

    private static final long serialVersionUID = 1L;

    private static final ListStateDescriptor<BucketEvent> COMMIT_MESSAGE_STATE_DESC =
            new ListStateDescriptor<>(
                    "commit-message",
                    BucketEvent.class);

    private transient BucketTracker bucketTracker;
    private final BucketListener bucketListener;

    private ListState<BucketEvent> commitMessageState;
    private List<BucketEvent> bucketEvents;

    private FileSystem fileSystem;

    public StreamingFileBucketCommitter(BucketListener bucketListener) {
        this.bucketListener = bucketListener;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        OperatorStateStore stateStore = context.getOperatorStateStore();
        commitMessageState = stateStore.getListState(COMMIT_MESSAGE_STATE_DESC);
        this.bucketEvents = new ArrayList<>();
        Iterable<BucketEvent> messageIterable = commitMessageState.get();
        Iterator<BucketEvent> iterator = messageIterable.iterator();
        while (iterator.hasNext()) {
            BucketEvent bucketEvent = iterator.next();
            this.bucketEvents.add(bucketEvent);
        }
        Configuration configuration = new Configuration();
        this.fileSystem = FileSystem.get(configuration);
    }

    @Override
    public void processElement(StreamRecord<BucketEvent<BucketId>> element) throws Exception {
        BucketEvent<BucketId> message = element.getValue();
        bucketEvents.add(message);
        if (bucketTracker == null) {
            bucketTracker = new BucketTracker(message.numberOfTasks);
        }
        boolean allBucketInactive = bucketTracker.add(message.bucketId, message.taskId);
        if (allBucketInactive) {
            Iterator<BucketEvent> iterator = bucketEvents.iterator();
            long records = 0L;
            while (iterator.hasNext()) {
                BucketEvent bucketEvent = iterator.next();
                if (bucketEvent.bucketId.equals(message.bucketId)) {
                    records += bucketEvent.records;
                    iterator.remove();
                }
            }
            // TODO notify listener
            Path bucketPath = new Path(message.path.getPath());
            ContentSummary contentSummary = fileSystem.getContentSummary(bucketPath);
            long bucketFileLength = contentSummary.getLength();
            long bucketFileCount = contentSummary.getFileCount();
            BucketInfo<BucketId> bucketInfo = new BucketInfo<>(
                    message.bucketId,
                    message.path,
                    records,
                    bucketFileLength,
                    bucketFileCount);
            bucketListener.notifyBucketInactive(bucketInfo);
            log.info("Bucket {} is already inactive, records {}, path {}, bucket file length {}, bucket file count {}",
                    message.bucketId, records, message.path, bucketFileLength, bucketFileCount);
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        commitMessageState.clear();
        commitMessageState.addAll(bucketEvents);
    }


    /**
     * BucketMessage消息跟踪器
     *
     * @param <BucketId>
     */
    private static class BucketTracker<BucketId> {

        private final int numberOfTasks;

        private TreeMap<BucketId, Set<Integer>> notifiedTasks = new TreeMap<>();

        private BucketTracker(int numberOfTasks) {
            this.numberOfTasks = numberOfTasks;
        }

        /**
         * 跟踪所有Bucket是否都关闭
         *
         * @param bucketId
         * @param task
         * @return 当所有并行度的task的bucketMessage都收到后返回true
         */
        private boolean add(BucketId bucketId, int task) {
            Set<Integer> tasks = notifiedTasks.computeIfAbsent(bucketId, (k) -> new HashSet<>());
            tasks.add(task);
            if (tasks.size() == numberOfTasks) {
                notifiedTasks.headMap(bucketId, true).clear();
                return true;
            }
            return false;
        }
    }

}
