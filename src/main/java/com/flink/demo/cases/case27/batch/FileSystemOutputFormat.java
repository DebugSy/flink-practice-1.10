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

import com.flink.demo.cases.case27.datastream.metastore.TableMetaStoreFactory;
import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.types.Row;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * File system {@link OutputFormat} for batch job. It commit in {@link #finalizeGlobal(int)}.
 */
public class FileSystemOutputFormat implements OutputFormat<Row>, FinalizeOnMaster, Serializable {

    private static final long serialVersionUID = 1L;

    private static final long CHECKPOINT_ID = 0;

    private final FileSystemFactory fsFactory;
    private final TableMetaStoreFactory msFactory;
    private final boolean overwrite;
    private final Path tmpPath;
    private final String[] columns;
    private final String[] partitionColumns;
    private final boolean dynamicGrouped;
    private final LinkedHashMap<String, String> staticPartitions;
    private final OutputFormatFactory<Row> formatFactory;
    private final OutputFileConfig outputFileConfig;

    private transient PartitionWriter<Row> writer;
    private transient Configuration parameters;

    private FileSystemOutputFormat(
            FileSystemFactory fsFactory,
            TableMetaStoreFactory msFactory,
            boolean overwrite,
            Path tmpPath,
            String[] columns,
            String[] partitionColumns,
            boolean dynamicGrouped,
            LinkedHashMap<String, String> staticPartitions,
            OutputFormatFactory<Row> formatFactory,
            OutputFileConfig outputFileConfig) {
        this.fsFactory = fsFactory;
        this.msFactory = msFactory;
        this.overwrite = overwrite;
        this.tmpPath = tmpPath;
        this.columns = columns;
        this.partitionColumns = partitionColumns;
        this.dynamicGrouped = dynamicGrouped;
        this.staticPartitions = staticPartitions;
        this.formatFactory = formatFactory;
        this.outputFileConfig = outputFileConfig;
    }

    @Override
    public void finalizeGlobal(int parallelism) {
        try {
            FileSystemCommitter committer = new FileSystemCommitter(
                    fsFactory,
                    msFactory,
                    overwrite,
                    tmpPath,
                    partitionColumns.length);
            committer.commitUpToCheckpoint(CHECKPOINT_ID);
        } catch (Exception e) {
            throw new TableException("Exception in finalizeGlobal", e);
        } finally {
            new File(tmpPath.getPath()).delete();
        }
    }

    @Override
    public void configure(Configuration parameters) {
        this.parameters = parameters;
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        try {
            PartitionTempFileManager fileManager = new PartitionTempFileManager(
                    fsFactory, tmpPath, taskNumber, CHECKPOINT_ID, outputFileConfig);
            PartitionWriter.Context<Row> context = new PartitionWriter.Context(
                    parameters, formatFactory);
            writer = PartitionWriterFactory.get(
                    partitionColumns.length - staticPartitions.size() > 0,
                    dynamicGrouped,
                    staticPartitions).create(context, fileManager, columns, partitionColumns);
        } catch (Exception e) {
            throw new TableException("Exception in open", e);
        }
    }

    @Override
    public void writeRecord(Row record) {
        try {
            writer.write(record);
        } catch (Exception e) {
            throw new TableException("Exception in writeRecord", e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            writer.close();
        } catch (Exception e) {
            throw new TableException("Exception in close", e);
        }
    }

    /**
     * Builder to build {@link FileSystemOutputFormat}.
     */
    public static class Builder {

        private String[] columns;
        private String[] partitionColumns;
        private OutputFormatFactory<Row> formatFactory;
        private TableMetaStoreFactory metaStoreFactory;
        private Path tmpPath;

        private LinkedHashMap<String, String> staticPartitions = new LinkedHashMap<>();
        private boolean dynamicGrouped = false;
        private boolean overwrite = false;
        private FileSystemFactory fileSystemFactory = FileSystem::get;

        private OutputFileConfig outputFileConfig = new OutputFileConfig("", "");

        public Builder setAllColumns(String[] columns) {
            this.columns = columns;
            return this;
        }

        public Builder setPartitionColumns(String[] partitionColumns) {
            this.partitionColumns = partitionColumns;
            return this;
        }

        public Builder setStaticPartitions(LinkedHashMap<String, String> staticPartitions) {
            this.staticPartitions = staticPartitions;
            return this;
        }

        public Builder setDynamicGrouped(boolean dynamicGrouped) {
            this.dynamicGrouped = dynamicGrouped;
            return this;
        }

        public Builder setFormatFactory(OutputFormatFactory<Row> formatFactory) {
            this.formatFactory = formatFactory;
            return this;
        }

        public Builder setFileSystemFactory(FileSystemFactory fileSystemFactory) {
            this.fileSystemFactory = fileSystemFactory;
            return this;
        }

        public Builder setMetaStoreFactory(TableMetaStoreFactory metaStoreFactory) {
            this.metaStoreFactory = metaStoreFactory;
            return this;
        }

        public Builder setOverwrite(boolean overwrite) {
            this.overwrite = overwrite;
            return this;
        }

        public Builder setTempPath(Path tmpPath) {
            this.tmpPath = tmpPath;
            return this;
        }

        public Builder setOutputFileConfig(OutputFileConfig outputFileConfig) {
            this.outputFileConfig = outputFileConfig;
            return this;
        }

        public FileSystemOutputFormat build() {
            checkNotNull(partitionColumns, "partitionColumns should not be null");
            checkNotNull(formatFactory, "formatFactory should not be null");
            checkNotNull(metaStoreFactory, "metaStoreFactory should not be null");
            checkNotNull(tmpPath, "tmpPath should not be null");

            return new FileSystemOutputFormat(
                    fileSystemFactory,
                    metaStoreFactory,
                    overwrite,
                    tmpPath,
                    columns,
                    partitionColumns,
                    dynamicGrouped,
                    staticPartitions,
                    formatFactory,
                    outputFileConfig);
        }
    }
}
