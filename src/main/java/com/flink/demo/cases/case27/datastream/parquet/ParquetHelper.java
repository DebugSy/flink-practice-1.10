package com.flink.demo.cases.case27.datastream.parquet;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParquetHelper {

    private static Pattern pattern = Pattern.compile("decimal\\((.*),(.*)\\)");

    /**
     * convert decimal to byte[]
     * @param bigDecimal
     * @param precision
     * @return
     */
    public static byte[] convertDecimalToBinary(BigDecimal bigDecimal, int precision) {
        byte[] bigDecimalBytes = bigDecimal.unscaledValue().toByteArray();
        byte[] decimalBytes;
        int minBytesForPrecision = computeMinBytesForPrecision(precision); //计算出合适的大小
        if (bigDecimalBytes.length == minBytesForPrecision) {
            decimalBytes = bigDecimalBytes;
        } else {
            byte signByte = bigDecimalBytes[0] < 0 ?
                    Integer.valueOf(-1).byteValue() :
                    Integer.valueOf(0).byteValue();
            byte[] decimalBuffer = new byte[minBytesForPrecision];
            Arrays.fill(
                    decimalBuffer,
                    0,
                    minBytesForPrecision - bigDecimalBytes.length,
                    signByte);
            System.arraycopy(
                    bigDecimalBytes,
                    0,
                    decimalBuffer,
                    minBytesForPrecision - bigDecimalBytes.length,
                    bigDecimalBytes.length);
            decimalBytes = decimalBuffer;
        }
        return decimalBytes;
    }

    /**
     * 根据decimal整数位长度计算存储需要最小的字节数
     * @param precision decimal的整数位长度
     * @return
     */
    public static int computeMinBytesForPrecision(int precision) {
        int numBytes = 1;
        while (Math.pow(2.0, 8 * numBytes - 1) < Math.pow(10.0, precision)) {
            numBytes += 1;
        }
        return numBytes;
    }

    /**
     * 提取decimal type
     * @param type
     * @return
     */
    public static DecimalContainer convertTypeDecimal(String type) {
        Matcher matcher = pattern.matcher(type);
        try {
            if (matcher.find() && matcher.groupCount() == 2) {
                int precision = Integer.parseInt(matcher.group(1));
                int scale = Integer.parseInt(matcher.group(2));
                DecimalContainer decimalContainer = new DecimalContainer(precision, scale);
                return decimalContainer;
            } else {
                throw new RuntimeException("decimal type argument error, type is " + type);
            }
        } catch (Exception e) {
            throw new RuntimeException("parse decimal type throw exception, type is " + type);
        }
    }

}
