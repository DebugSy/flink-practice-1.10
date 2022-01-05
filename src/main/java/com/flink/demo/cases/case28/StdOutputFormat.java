package com.flink.demo.cases.case28;

import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.io.IOException;

public class StdOutputFormat implements OutputFormat<Row>, FinalizeOnMaster {

    @Override
    public void configure(Configuration parameters) {
        System.err.println("OutputFormat configuration.");
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        System.err.println("OutputFormat open......");
    }

    @Override
    public void writeRecord(Row record) throws IOException {
        System.err.println(record);
    }

    @Override
    public void close() throws IOException {
        System.err.println("OutputFormat closed.");
    }

    @Override
    public void finalizeGlobal(int parallelism) throws IOException {
        System.err.println("Job global finalized");
    }
}
