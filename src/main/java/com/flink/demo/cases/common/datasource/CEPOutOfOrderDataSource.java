package com.flink.demo.cases.common.datasource;

import com.flink.demo.cases.common.utils.TypeInfoUtil;
import lombok.Getter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 复杂事件乱序数据源
 */
public class CEPOutOfOrderDataSource extends RichSourceFunction<Row> {

    private static final Logger logger = LoggerFactory.getLogger(CEPOutOfOrderDataSource.class);

    public static String CLICK_FIELDS = "userId,username,url,clickTime,rowtime.rowtime";

    public static TypeInformation<Row> CLICK_TYPEINFO = Types.ROW(
            new String[]{"userId", "username", "url", "clickTime"},
            new TypeInformation[]{
                    Types.INT(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.SQL_TIMESTAMP()
            });

    private static RowTypeInfo clickTypeinfo = (RowTypeInfo) CLICK_TYPEINFO;

    private boolean running = true;

    @Getter
    private List<List<String>> clicks = new ArrayList<>();

    public CEPOutOfOrderDataSource(String dataFilePath) {
        File file = new File(dataFilePath);
        try {
            String dataStr = FileUtils.readFile(file, "UTF-8");
            String[] strings = dataStr.split("\n");
            for (int i = 0; i < strings.length; i++) {
                String str = strings[i];
                String[] splits = str.split(",");
                List<String> stringsList = new ArrayList<>();
                for (int j = 0; j < splits.length; j++) {
                    stringsList.add(splits[j]);
                }
                clicks.add(stringsList);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run(SourceContext<Row> sourceContext) throws Exception {
        Iterator<List<String>> iterator = clicks.iterator();
        while (iterator.hasNext()) {
            List<String> record = iterator.next();
            Row row = new Row(record.size());
            for (int i = 0; i < record.size(); i++) {
                Object value = TypeInfoUtil.cast(record.get(i), clickTypeinfo.getTypeAt(i));
                row.setField(i, value);
            }
            logger.info("emit {} -> {}", Timestamp.valueOf(record.get(3)).getTime(), row);
            sourceContext.collect(row);

            Thread.sleep(300 * 1);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}