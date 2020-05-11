package com.flink.demo.cases.common.datasource;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * CEP 测试数据，计算5分钟内同一用户两次交易，一次金额大于98，另一次金额小于3，输出告警信息（不区分交易是否成功，所有交易全部纳入统计）
 *
 * 表:
 * CDR_ID 流水号 主键
 * USR_ID 用户号
 * USER_BRAND 商户号
 * ORD_STS 订单状态 0交易成功 1等待 2订单取消
 * RCV_AMT 交易金额
 * OFF_FLAG 营销标识
 * POST_TM 交易时间
 */
public class CEPDataSource extends RichSourceFunction<Row> {

    private static final Logger logger = LoggerFactory.getLogger(CEPDataSource.class);

    public static String CLICK_FIELDS = "CDR_ID,USR_ID,USER_BRAND,ORD_STS,RCV_AMT,OFF_FLAG,POST_TM";

    public static String CLICK_FIELDS_PROCTIME = "CDR_ID,USR_ID,USER_BRAND,ORD_STS,RCV_AMT,OFF_FLAG,POST_TM.rowtime";

    public static String CLICK_FIELDS_truncatedTime = "CDR_ID,USR_ID,USER_BRAND,ORD_STS,RCV_AMT,OFF_FLAG,POST_TM,truncatedTime,procTime.procTime";

    public static TypeInformation CLICK_TYPEINFO = Types.ROW(
            new String[]{"CDR_ID", "USR_ID", "USER_BRAND", "ORD_STS", "RCV_AMT", "OFF_FLAG", "POST_TM"},
            new TypeInformation[]{
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.INT(),
                    Types.DECIMAL(),
                    Types.INT(),
                    Types.LONG()
            });

    private boolean running = true;

    private static List<List<String>> clicks = new ArrayList<>();

    private static final Row row = new Row(7);

    static {
        clicks.add(Arrays.asList("61", "user03", "brand12", "0", "51.90", "1", "2019-07-23 23:27:29.876"));
        clicks.add(Arrays.asList("62", "user05", "brand14", "0", "11.90", "1", "2019-07-23 23:27:30.851"));
        clicks.add(Arrays.asList("63", "user02", "brand16", "0", "99.90", "", "2019-07-23 23:27:31.840"));
        clicks.add(Arrays.asList("633", "user02", "brand16", "0", "0.90", "1",  "2019-07-23 23:27:31.851"));

        clicks.add(Arrays.asList("64", "user05", "brand11", "0", "41.90", "1", "2019-07-23 23:27:31.893"));
        clicks.add(Arrays.asList("65", "user03", "brand12", "1", "54.90", "1", "2019-07-23 23:27:32.897"));
        clicks.add(Arrays.asList("66", "user07", "brand13", "0", "99.10", "1", "2019-07-23 23:27:32.901"));
        clicks.add(Arrays.asList("67", "user08", "brand16", "0", "61.90", "1", "2019-07-23 23:27:36.908"));
        clicks.add(Arrays.asList("68", "user04", "brand12", "0", "1.90",  "", "2019-07-23 23:27:36.915"));
//        clicks.add(Arrays.asList("69", "user06", "brand18", "0", "41.90", "1", "2019-07-23 23:27:27.019"));//晚到数据
        clicks.add(Arrays.asList("70", "user01", "brand16", "0", "69.90", "1", "2019-07-23 23:27:38.916"));
//        clicks.add(Arrays.asList("71", "user09", "brand19", "0", "58.90", "1", "2019-07-23 23:27:27.189"));//晚到数据
        clicks.add(Arrays.asList("72", "user06", "brand14", "0", "43.90", "1", "2019-07-23 23:27:48.950"));
        clicks.add(Arrays.asList("73", "user02", "brand17", "1", "12.90", "1", "2019-07-23 23:27:53.960"));
        clicks.add(Arrays.asList("733", "user03", "brand19", "1", "98.90", "1", "2019-07-23 23:28:01.960"));
        clicks.add(Arrays.asList("74", "user04", "brand19", "0", "3.90",  "", "2019-07-23 23:28:02.990"));
//        clicks.add(Arrays.asList("75", "user07", "brand13", "0", "99.90", "1", "2019-07-23 23:27:30.960"));//晚到数据
        clicks.add(Arrays.asList("76", "user05", "brand15", "0", "10.90", "1", "2019-07-23 23:28:08.012"));
        clicks.add(Arrays.asList("77", "user03", "brand18", "2", "7.90",  "1", "2019-07-23 23:28:12.029"));
        clicks.add(Arrays.asList("777", "user03", "brand16", "2", "2.90",  "1", "2019-07-23 23:28:12.198"));

//        clicks.add(Arrays.asList("78", "user04", "brand12", "0", "9.90",  "1", "2019-07-23 23:27:39.918"));//晚到数据
//        clicks.add(Arrays.asList("79", "user02", "brand16", "0", "63.90", "1", "2019-07-23 23:27:43.931"));//晚到数据
        clicks.add(Arrays.asList("80", "user01", "brand13", "2", "78.90", "", "2019-07-23 23:28:20.000"));
        clicks.add(Arrays.asList("81", "user06", "brand12", "0", "21.90", "1", "2019-07-23 23:28:20.001"));
        clicks.add(Arrays.asList("82", "user08", "brand11", "0", "46.90", "1", "2019-07-23 23:28:20.581"));
        clicks.add(Arrays.asList("822", "user07", "brand11", "0", "2.90", "1", "2019-07-23 23:28:21.591"));

    }

    @Override
    public void run(SourceContext<Row> sourceContext) throws Exception {
        Iterator<List<String>> iterator = clicks.iterator();
        while (iterator.hasNext()) {
            List<String> record = iterator.next();

            row.setField(0, record.get(0));
            row.setField(1, record.get(1));
            row.setField(2, record.get(2));
            row.setField(3, Integer.parseInt(record.get(3)));
            row.setField(4, BigDecimal.valueOf(Double.parseDouble(record.get(4))));
            row.setField(5, Strings.isNullOrEmpty(record.get(5)) ? null : Integer.parseInt(record.get(5)));
            row.setField(6, Timestamp.valueOf(record.get(6)).getTime());

            System.out.println();
            logger.info("emit {} -> {}", Timestamp.valueOf(record.get(6)).getTime(), row);
            sourceContext.collect(row);

            Thread.sleep(1000 * 1);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}