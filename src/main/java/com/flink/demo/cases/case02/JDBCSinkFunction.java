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

package com.flink.demo.cases.case02;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Slf4j
public class JDBCSinkFunction extends RichSinkFunction<Row> implements CheckpointedFunction, CheckpointListener {
	final JDBCOutputFormat outputFormat;

	private ListState<Row> checkpointedState;

	private List<Row> bufferedElements;

	private final int batchInterval;

	public JDBCSinkFunction(JDBCOutputFormat outputFormat) {
		this.outputFormat = outputFormat;
		this.batchInterval = 10;
		this.bufferedElements = new ArrayList<>();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		RuntimeContext ctx = getRuntimeContext();
		outputFormat.setRuntimeContext(ctx);
		outputFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());
	}

	@Override
	public void invoke(Row value, Context context) throws Exception {
		bufferedElements.add(value);
		log.debug("write row {}", value);
		if (bufferedElements.size() >= batchInterval) {
			// execute batch
			log.debug("threshold {} trigger flush, current batch size {}", batchInterval, bufferedElements.size());
			Iterator<Row> iterator = bufferedElements.iterator();
			while (iterator.hasNext()) {
				Row row = iterator.next();
				outputFormat.writeRecord(row);
				iterator.remove();
			}
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		log.debug("snapshot checkpoint state");
		checkpointedState.addAll(bufferedElements);
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		log.debug("init checkpoint state");
		ListStateDescriptor<Row> cacheDesc = new ListStateDescriptor<>("cache", Row.class);
		this.checkpointedState = context.getOperatorStateStore().getListState(cacheDesc);
		Iterable<Row> iterable = this.checkpointedState.get();
		for (Row row : iterable) {
			System.err.println("recovery data " + row);
			bufferedElements.add(row);
		}
	}

	@Override
	public void close() throws Exception {
		log.debug("sink function closing......");
		outputFormat.close();
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		System.err.println("checkpoint complete");
		outputFormat.commit();
	}
}
