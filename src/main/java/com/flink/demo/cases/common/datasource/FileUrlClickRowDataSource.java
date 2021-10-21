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
public class FileUrlClickRowDataSource extends RichParallelSourceFunction<Row> {

    private static final Logger logger = LoggerFactory.getLogger(FileUrlClickRowDataSource.class);

    private volatile boolean running = true;

    public static TypeInformation USER_CLICK_TYPEINFO = Types.ROW(
            new String[]{"userId", "username", "url", "clickTime", "rank", "uuid", "date_col", "time_col"},
            new TypeInformation[]{
                    Types.INT(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.SQL_TIMESTAMP(),
                    Types.INT(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING()
            });

    private static final String d = "98,userA_603c,http://127.0.0.1/api/H,2020-09-24 09:46:02.779,45,7dd188da-63b1-406d-9563-4f0731070c77,20200924,094602";

    private static final String dataFilePath = "/data/UrlClickData_Username_UUID.csv";

    private BufferedReader reader;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.reader = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream(dataFilePath)));
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        while (running) {
            String s = null;
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
        Row row = new Row(8);
        row.setField(0, Integer.parseInt(columnValues[0]));
        row.setField(1, columnValues[1]);
        row.setField(2, columnValues[2]);
        row.setField(3, Timestamp.valueOf(columnValues[3]));
        row.setField(4, Integer.parseInt(columnValues[4]));
        row.setField(5, columnValues[5]);
        row.setField(6, columnValues[6]);
        row.setField(7, columnValues[7]);
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

