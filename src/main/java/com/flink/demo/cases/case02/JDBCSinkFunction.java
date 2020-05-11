package com.flink.demo.cases.case02;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.sql.Types;

public class JDBCSinkFunction extends RichSinkFunction<Row> {

    private JDBCOutputFormat outputFormat;

    @Override
    public void open(Configuration parameters) throws Exception {
        JDBCOutputFormat format = JDBCOutputFormat.buildJDBCOutputFormat()
                .setUsername("root")
                .setPassword("")
                .setDBUrl("jdbc:mysql://localhost:3306/test")
                .setQuery("insert into url_click(userId,username,url,clickTime,data_col,time_col) values (?,?,?,?,?,?)")
                .setDrivername("com.mysql.jdbc.Driver")
                .setBatchInterval(1)
                .setSqlTypes(new int[]{Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR})
                .finish();
        this.outputFormat = format;
        RuntimeContext ctx = getRuntimeContext();
        this.outputFormat.setRuntimeContext(ctx);
        this.outputFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());
    }


    @Override
    public void invoke(Row value, Context context) throws Exception {
        outputFormat.writeRecord(value);
    }
}
