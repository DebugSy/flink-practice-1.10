package com.flink.demo.cases.case08;

import org.apache.flink.formats.parquet.ParquetBuilder;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;

public class ParquetBuilderImpl implements ParquetBuilder {

    @Override
    public ParquetWriter createWriter(OutputFile out) throws IOException {
        return null;
    }
    
}
