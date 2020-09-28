package com.flink.demo.cases.case12;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

public class ConsoleTableSink implements AppendStreamTableSink<Row> {

    private final RowTypeInfo rowTypeInfo;

    public ConsoleTableSink(RowTypeInfo rowTypeInfo) {
        this.rowTypeInfo = rowTypeInfo;
    }

    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        dataStream.printToErr();
    }

    @Override
    public TypeInformation<Row> getOutputType() {
        return rowTypeInfo;
    }

    @Override
    public String[] getFieldNames() {
        return rowTypeInfo.getFieldNames();
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return rowTypeInfo.getFieldTypes();
    }

    @Override
    public TableSink<Row> configure(String[] strings, TypeInformation<?>[] typeInformations) {
        return new ConsoleTableSink(rowTypeInfo);
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
        return dataStream.addSink(new SinkFunction<Row>() {
            @Override
            public void invoke(Row value, Context context) throws Exception {
                System.err.println(value);
            }
        });
    }
}
