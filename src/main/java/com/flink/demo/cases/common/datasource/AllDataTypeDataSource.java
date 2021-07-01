package com.flink.demo.cases.common.datasource;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

public class AllDataTypeDataSource extends RichParallelSourceFunction<Row> {

    private static final Logger logger = LoggerFactory.getLogger(AllDataTypeDataSource.class);

    private volatile boolean running = true;

    public static TypeInformation<Row> allDataTypeInfo = Types.ROW(
            new String[]{
                    "int_col",
                    "string_col",
                    "ts"
            },
            new TypeInformation[]{
                    Types.INT(),
                    Types.STRING(),
                    Types.SQL_TIMESTAMP()
            });

    private final Row row = new Row(3);

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        Random random = new Random(System.currentTimeMillis());
        int count = 0;
        Thread.sleep(1000 * 5);
        while (running) {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            Thread.sleep((indexOfThisSubtask + 1) * 1000);

            int intColV = random.nextInt(10000);
            String stringColV = "string_col_value_" + intColV;
            boolean booleanColV = random.nextBoolean();
            byte byteColV = Byte.MAX_VALUE;
            Timestamp timestampColV = new Timestamp(System.currentTimeMillis());
            Date dateColV = new Date(timestampColV.getTime());
            BigDecimal decimalColV = generateDecimal(random.nextDouble(), 0);
            double doubleColV = random.nextDouble();
            float floatColV = random.nextFloat();
            long longColV = random.nextLong();
            short shortCloV = Short.MAX_VALUE;
            byte[] binaryColV = new byte[8];
            Arrays.fill(binaryColV, Byte.MIN_VALUE);

            row.setField(0, intColV);
            row.setField(1, stringColV);
            row.setField(2, timestampColV);
            logger.info("emit -> {}", row);
            ctx.collect(row);
            count++;
            if (count == 10) {
//                running = false;
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    /**
     * 生成bigDecimal类型
     * @param number 数值
     * @param scale 小数位数
     * @return
     */
    private BigDecimal generateDecimal(double number, int scale) {
        NumberFormat numberFormat = NumberFormat.getNumberInstance();
        numberFormat.setMaximumFractionDigits(scale);
        String decimalCol1Format = numberFormat.format(number);
        BigDecimal bigDecimal = new BigDecimal(decimalCol1Format);
        return bigDecimal;
    }
}
