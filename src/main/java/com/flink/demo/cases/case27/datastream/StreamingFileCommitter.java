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

package com.flink.demo.cases.case27.datastream;

import com.flink.demo.cases.case27.datastream.metastore.HiveTableMetaStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.filesystem.PartitionCommitPolicy;
import org.apache.flink.table.filesystem.stream.PartitionCommitTrigger;
import org.apache.flink.table.filesystem.stream.StreamingFileWriter;

import java.io.Serializable;
import java.util.*;

import static org.apache.flink.table.utils.PartitionPathUtils.extractPartitionSpecFromPath;
import static org.apache.flink.table.utils.PartitionPathUtils.generatePartitionPath;

/**
 * Committer for {@link StreamingFileWriter}. This is the single (non-parallel) task.
 * It collects all the partition information sent from upstream, and triggers the partition
 * submission decision when it judges to collect the partitions from all tasks of a checkpoint.
 *
 * <p>Processing steps:
 * 1.Partitions are sent from upstream. Add partition to trigger.
 * 2.{@link TaskTracker} say it have already received partition data from all tasks in a checkpoint.
 * 3.Extracting committable partitions from {@link PartitionCommitTrigger}.
 * 4.Using {@link PartitionCommitPolicy} chain to commit partitions.
 *
 * <p>See {@link StreamingFileWriter#notifyCheckpointComplete}.
 */
public class StreamingFileCommitter extends AbstractStreamOperator<Void>
		implements OneInputStreamOperator<StreamingFileCommitter.CommitMessage, Void> {

	private static final long serialVersionUID = 1L;

	private final Configuration conf;

	private final String hiveVersion;

	private final String database;

	private final String table;

	private final Path locationPath;

	private final List<String> partitionKeys;

	private transient PartitionCommitTrigger trigger;

	private transient TaskTracker taskTracker;

	private transient long currentWatermark;

	public StreamingFileCommitter(
			String database,
			String table,
			String hiveVersion,
			Path locationPath,
			List<String> partitionKeys,
			Configuration conf) {
		this.hiveVersion = hiveVersion;
		this.database = database;
		this.table = table;
		this.locationPath = locationPath;
		this.partitionKeys = partitionKeys;
		this.conf = conf;
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		currentWatermark = Long.MIN_VALUE;
		this.trigger = PartitionCommitTrigger.create(
				context.isRestored(),
				context.getOperatorStateStore(),
				conf,
				getUserCodeClassloader(),
				partitionKeys,
				getProcessingTimeService());
	}

	@Override
	public void processElement(StreamRecord<CommitMessage> element) throws Exception {
		CommitMessage message = element.getValue();
		for (String partition : message.partitions) {
			trigger.addPartition(partition);
		}

		if (taskTracker == null) {
			taskTracker = new TaskTracker(message.numberOfTasks);
		}
		boolean needCommit = taskTracker.add(message.checkpointId, message.taskId);
		if (needCommit) {
			commitPartitions(message.checkpointId);
		}
	}

	private void commitPartitions(long checkpointId) throws Exception {
		List<String> partitions = checkpointId == Long.MAX_VALUE ?
				trigger.endInput() :
				trigger.committablePartitions(checkpointId);
		if (partitions.isEmpty()) {
			return;
		}

		try (HiveTableMetaStore metaStore = new HiveTableMetaStore(database, table, hiveVersion)){
			for (String partition : partitions) {
				LinkedHashMap<String, String> partSpec = extractPartitionSpecFromPath(new Path(partition));
				LOG.info("Partition {} of table {} is ready to be committed", partSpec, table);
				Path path = new Path(locationPath, generatePartitionPath(partSpec));
				metaStore.createOrAlterPartition(partSpec, path);
			}
		}
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		super.processWatermark(mark);
		this.currentWatermark = mark.getTimestamp();
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);
		trigger.snapshotState(context.getCheckpointId(), currentWatermark);
	}

	/**
	 * The message sent upstream.
	 *
	 * <p>Need to ensure that the partitions are ready to commit. That is to say, the files in
	 * the partition have become readable rather than temporary.
	 */
	public static class CommitMessage implements Serializable {

		public long checkpointId;
		public int taskId;
		public int numberOfTasks;
		public List<String> partitions;

		/**
		 * Pojo need this constructor.
		 */
		public CommitMessage() {}

		public CommitMessage(
				long checkpointId, int taskId, int numberOfTasks, List<String> partitions) {
			this.checkpointId = checkpointId;
			this.taskId = taskId;
			this.numberOfTasks = numberOfTasks;
			this.partitions = partitions;
		}
	}

	/**
	 * Track the upstream tasks to determine whether all the upstream data of a checkpoint
	 * has been received.
	 */
	private static class TaskTracker {

		private final int numberOfTasks;

		/**
		 * Checkpoint id to notified tasks.
		 */
		private TreeMap<Long, Set<Integer>> notifiedTasks = new TreeMap<>();

		private TaskTracker(int numberOfTasks) {
			this.numberOfTasks = numberOfTasks;
		}

		/**
		 * @return true, if this checkpoint id need be committed.
		 */
		private boolean add(long checkpointId, int task) {
			Set<Integer> tasks = notifiedTasks.computeIfAbsent(checkpointId, (k) -> new HashSet<>());
			tasks.add(task);
			if (tasks.size() == numberOfTasks) {
				notifiedTasks.headMap(checkpointId, true).clear();
				return true;
			}
			return false;
		}
	}
}
