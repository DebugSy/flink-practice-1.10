/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flink.demo.cases.case27.datastream.bucket;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.sink.filesystem.*;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.filesystem.stream.StreamingFileCommitter;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Operator for file system sink. It is a operator version of {@link StreamingFileSink}.
 * It sends partition commit message to downstream for committing.
 *
 * <p>See {@link StreamingFileCommitter}.
 */
public class StreamingFileWriter extends AbstractStreamOperator<com.flink.demo.cases.case27.datastream.StreamingFileCommitter.CommitMessage>
		implements OneInputStreamOperator<Row, com.flink.demo.cases.case27.datastream.StreamingFileCommitter.CommitMessage>, BoundedOneInput{

	private static final long serialVersionUID = 1L;

	// ------------------------ configuration fields --------------------------

	private final long bucketCheckInterval;

	private final StreamingFileSink.BucketsBuilder<Row, String, ? extends
			StreamingFileSink.BucketsBuilder<Row, String, ?>> bucketsBuilder;

	// --------------------------- runtime fields -----------------------------

	private transient Buckets<Row, String> buckets;

	private transient StreamingFileSinkHelper<Row> helper;

	private transient long currentWatermark;

	private transient Set<String> inactivePartitions;

	public StreamingFileWriter(
			long bucketCheckInterval,
			StreamingFileSink.BucketsBuilder<Row, String, ? extends
					StreamingFileSink.BucketsBuilder<Row, String, ?>> bucketsBuilder) {
		this.bucketCheckInterval = bucketCheckInterval;
		this.bucketsBuilder = bucketsBuilder;
		setChainingStrategy(ChainingStrategy.ALWAYS);
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		buckets = bucketsBuilder.createBuckets(getRuntimeContext().getIndexOfThisSubtask());

		// Set listener before the initialization of Buckets.
		inactivePartitions = new HashSet<>();
		buckets.setBucketLifeCycleListener(new BucketLifeCycleListener<Row, String>() {
			@Override
			public void bucketCreated(Bucket<Row, String> bucket) {
			}

			@Override
			public void bucketInactive(Bucket<Row, String> bucket) {
				inactivePartitions.add(bucket.getBucketId());
			}
		});

		helper = new StreamingFileSinkHelper<>(
				buckets,
				context.isRestored(),
				context.getOperatorStateStore(),
				getRuntimeContext().getProcessingTimeService(),
				bucketCheckInterval);
		currentWatermark = Long.MIN_VALUE;
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);
		helper.snapshotState(context.getCheckpointId());
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		super.processWatermark(mark);
		currentWatermark = mark.getTimestamp();
	}

	@Override
	public void processElement(StreamRecord<Row> element) throws Exception {
		helper.onElement(
				element.getValue(),
				getProcessingTimeService().getCurrentProcessingTime(),
				element.hasTimestamp() ? element.getTimestamp() : null,
				currentWatermark);
	}

	/**
	 * Commit up to this checkpoint id, also send inactive partitions to downstream for committing.
	 */
	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		super.notifyCheckpointComplete(checkpointId);
		commitUpToCheckpoint(checkpointId);
	}

	private void commitUpToCheckpoint(long checkpointId) throws Exception {
		helper.commitUpToCheckpoint(checkpointId);
		com.flink.demo.cases.case27.datastream.StreamingFileCommitter.CommitMessage message =
				new com.flink.demo.cases.case27.datastream.StreamingFileCommitter.CommitMessage(
				checkpointId,
				getRuntimeContext().getIndexOfThisSubtask(),
				getRuntimeContext().getNumberOfParallelSubtasks(),
				new ArrayList<>(inactivePartitions));
		output.collect(new StreamRecord<>(message));
		inactivePartitions.clear();
	}

	@Override
	public void endInput() throws Exception {
		buckets.onProcessingTime(Long.MAX_VALUE);
		helper.snapshotState(Long.MAX_VALUE);
		output.emitWatermark(new Watermark(Long.MAX_VALUE));
		commitUpToCheckpoint(Long.MAX_VALUE);
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();
		if (helper != null) {
			helper.close();
		}
	}
}
