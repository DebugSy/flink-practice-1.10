package com.flink.demo.cases.case27.datastream.bucket;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.types.Row;

import java.io.IOException;

public class ProjectionBulkFactory implements BulkWriter.Factory<Row>{

    private final BulkWriter.Factory<Row> factory;

    private final int[] nonPartitionIndexes;

    public ProjectionBulkFactory(BulkWriter.Factory<Row> factory, int[] nonPartitionIndexes) {
        this.factory = factory;
        this.nonPartitionIndexes = nonPartitionIndexes;
    }

    @Override
    public BulkWriter<Row> create(FSDataOutputStream out) throws IOException {
        BulkWriter<Row> writer = factory.create(out);
        return new BulkWriter<Row>() {

            @Override
            public void addElement(Row element) throws IOException {
                writer.addElement(Row.project(element, nonPartitionIndexes));
            }

            @Override
            public void flush() throws IOException {
                writer.flush();
            }

            @Override
            public void finish() throws IOException {
                writer.finish();
            }
        };
    }
}
