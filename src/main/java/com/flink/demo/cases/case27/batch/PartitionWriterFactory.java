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

package com.flink.demo.cases.case27.batch;

import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.LinkedHashMap;

/**
 * Factory of {@link PartitionWriter} to avoid virtual function calls.
 */
public interface PartitionWriterFactory extends Serializable {

    PartitionWriter<Row> create(
            PartitionWriter.Context<Row> context,
            PartitionTempFileManager manager,
            String[] columnNames,
            String[] partitionColumns) throws Exception;

    /**
     * Util for get a {@link PartitionWriterFactory}.
     */
    static PartitionWriterFactory get(
            boolean dynamicPartition,
            boolean grouped,
            LinkedHashMap<String, String> staticPartitions) {
        if (dynamicPartition) {
            return grouped ? GroupedPartitionWriter::new : DynamicPartitionWriter::new;
        } else {
            return (PartitionWriterFactory) (context, manager, columnNames, partitionColumns) ->
                    new SingleDirectoryWriter(context, manager, columnNames, partitionColumns, staticPartitions);
        }
    }
}
