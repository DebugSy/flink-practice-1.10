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
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.types.Row;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.utils.PartitionPathUtils.generatePartitionPath;

/**
 * Dynamic partition writer to writing multiple partitions at the same time, it maybe consumes more memory.
 */
@Slf4j
public class DynamicPartitionWriter implements PartitionWriter<Row> {

    private final Context<Row> context;
    private final PartitionTempFileManager manager;
    private final String[] columnNames;
    private final String[] partitionColumns;
    private final int[] partitionIndexes;
    private final int[] nonPartitionIndexes;
    private final Map<String, OutputFormat<Row>> formats;

    public DynamicPartitionWriter(
            Context<Row> context,
            PartitionTempFileManager manager,
            String[] columnNames,
            String[] partitionColumns) {
        log.info("Use partition writer {}", this.getClass().getSimpleName());
        this.context = context;
        this.manager = manager;
        this.columnNames = columnNames;
        this.partitionColumns = partitionColumns;
        List<String> columnList = Arrays.asList(columnNames);
        this.partitionIndexes = Arrays.stream(partitionColumns).mapToInt(columnList::indexOf).toArray();
        List<Integer> partitionIndexList = Arrays.stream(partitionIndexes).boxed().collect(Collectors.toList());
        this.nonPartitionIndexes = IntStream.range(0, columnNames.length)
                .filter(c -> !partitionIndexList.contains(c))
                .toArray();
        this.formats = new HashMap<>();
    }

    @Override
    public void write(Row in) throws Exception {
        LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();
        for (int i = 0; i < partitionIndexes.length; i++) {
            int index = partitionIndexes[i];
            Object field = in.getField(index);
            String partitionValue = field != null ? field.toString() : null;
            if (StringUtils.isEmpty(partitionValue)) {
                partitionValue = "__HIVE_DEFAULT_PARTITION__";
            }
            partSpec.put(partitionColumns[i], partitionValue);
        }
        String partition = generatePartitionPath(partSpec);
        OutputFormat<Row> format = formats.get(partition);

        if (format == null) {
            // create a new format to write new partition.
            format = context.createNewOutputFormat(manager.createPartitionDir(partition));
            formats.put(partition, format);
        }
        Row row = partitionIndexes.length == 0 ? in : Row.project(in, nonPartitionIndexes);
        format.writeRecord(row);
    }

    @Override
    public void close() throws Exception {
        for (OutputFormat<?> format : formats.values()) {
            format.close();
        }
        formats.clear();
    }
}
