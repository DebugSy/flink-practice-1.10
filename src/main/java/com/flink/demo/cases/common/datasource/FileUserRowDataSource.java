package com.flink.demo.cases.common.datasource;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Timestamp;

/**
 * Created by P0007 on 2019/9/3.
 */
public class FileUserRowDataSource extends RichParallelSourceFunction<Row> {

    private static final Logger logger = LoggerFactory.getLogger(FileUserRowDataSource.class);

    private volatile boolean running = true;

    public static TypeInformation USER_TYPEINFO = Types.ROW(
            new String[]{"userId", "username", "address", "activityTime"},
            new TypeInformation[]{
                    Types.INT(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.SQL_TIMESTAMP()
            });

    private static final String d = "72,userH_cef2,北京市朝阳区望京东湖街道7号,2020-09-24 09:46:00.056";

    private static final String dataFilePath = "/data/UserData.csv";

    private BufferedReader reader;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.reader = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream(dataFilePath)));
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        while (running) {
            String s = null;
            Thread.sleep(1000 * 10);
            while ((s = reader.readLine()) != null) {
                Row row = parserDataToRow(s);
                logger.info("emit {}", row);
                ctx.collect(row);
                Thread.sleep(1000 * 1);
            }
//            Row row = parserDataToRow(d);
//            ctx.collect(row);
            running = false;
        }
    }

    private Row parserDataToRow(String data) throws InterruptedException {
        String[] columnValues = data.split(",");
        Row row = new Row(4);
        row.setField(0, Integer.parseInt(columnValues[0]));
        row.setField(1, columnValues[1]);
        row.setField(2, columnValues[2]);
        row.setField(3, Timestamp.valueOf(columnValues[3]));
        return row;
    }

    @Override
    public void cancel() {
        running = false;
        try {
            reader.close();
        } catch (Exception e) {
            logger.error("close");
        }
    }

}

