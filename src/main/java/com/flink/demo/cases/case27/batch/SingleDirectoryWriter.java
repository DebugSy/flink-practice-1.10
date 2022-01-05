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

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.utils.PartitionPathUtils.generatePartitionPath;

/**
 * {@link org.apache.flink.table.filesystem.PartitionWriter} for single directory writer. It just use one format to write.
 *
 */
@Slf4j
public class SingleDirectoryWriter implements PartitionWriter<Row> {

    private final Context<Row> context;
    private final PartitionTempFileManager manager;
    private final int[] partitionIndexes;
    private final int[] nonPartitionIndexes;
    private final LinkedHashMap<String, String> staticPartitions;

    private OutputFormat<Row> format;

    public SingleDirectoryWriter(
            Context<Row> context,
            PartitionTempFileManager manager,
            String[] columnNames,
            String[] partitionColumns,
            LinkedHashMap<String, String> staticPartitions) {
        log.info("Use partition writer {}", this.getClass().getSimpleName());
        this.context = context;
        this.manager = manager;
        List<String> columnList = Arrays.asList(columnNames);
        this.partitionIndexes = Arrays.stream(partitionColumns).mapToInt(columnList::indexOf).toArray();
        List<Integer> partitionIndexList = Arrays.stream(partitionIndexes).boxed().collect(Collectors.toList());
        this.nonPartitionIndexes = IntStream.range(0, columnNames.length)
                .filter(c -> !partitionIndexList.contains(c))
                .toArray();
        this.staticPartitions = staticPartitions;
    }

    private void createFormat() throws IOException {
        this.format = context.createNewOutputFormat(staticPartitions.size() == 0 ?
                manager.createPartitionDir() :
                manager.createPartitionDir(generatePartitionPath(staticPartitions)));
    }

    @Override
    public void write(Row in) throws Exception {
        if (format == null) {
            createFormat();
        }
        Row row = partitionIndexes.length == 0 ? in : Row.project(in, nonPartitionIndexes);
        format.writeRecord(row);
    }

    @Override
    public void close() throws Exception {
        if (format != null) {
            format.close();
            format = null;
        }
    }
}
