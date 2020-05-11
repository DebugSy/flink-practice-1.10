package com.flink.demo.cases.common.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.shaded.org.joda.time.format.DateTimeFormat;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by joey on 2017/3/21.
 */
@Slf4j
public final class ClassUtil {

    private static Map<String, DateTimeFormatter> DatePatternMap = new ConcurrentHashMap<>();

    public static TypeInformation<?>[] toTypeInformations(String[] dataTypes) {
        TypeInformation<?>[] typeInformations = new TypeInformation[dataTypes.length];
        for (int i = 0; i < dataTypes.length; i++) {
            typeInformations[i] = TypeExtractor.getForClass(convert(dataTypes[i]));
        }
        return typeInformations;
    }

    public static <T> T convert(String value, Class<?> targetClass) {
        return (T) convert(value, targetClass.getSimpleName());
    }

    public static Object convert(String value, String dataType) {
        String type = dataType.toLowerCase();
        int idx = type.indexOf("(");
        if (idx > -1) {
            type = type.substring(0, idx);
        }
        switch (type) {
            case "string":
                return StringUtils.isEmpty(value) ? "" : value.trim();
            case "byte":
                return StringUtils.isEmpty(value) ? "" : Byte.parseByte(value);
            case "short":
                return StringUtils.isEmpty(value) ? 0 : Short.parseShort(value);
            case "integer":
            case "int":
                return StringUtils.isEmpty(value) ? 0 : Integer.parseInt(value);
            case "bigint":
            case "long":
                return StringUtils.isEmpty(value) ? 0l : Long.parseLong(value);
            case "float":
                return StringUtils.isEmpty(value) ? 0f : Float.parseFloat(value);
            case "double":
                return StringUtils.isEmpty(value) ? 0.0 : Double.parseDouble(value);
            case "bool":
            case "boolean":
                return StringUtils.isEmpty(value) ? false : Boolean.parseBoolean(value);
            case "date":
            case "datetype":
                return StringUtils.isEmpty(value) ? null : Date.valueOf(value);
            case "timestamp":
                return StringUtils.isEmpty(value) ? null : Timestamp.valueOf(value);
            case "binary":
                return StringUtils.isEmpty(value) ? null : Byte.parseByte(value);
            case "decimal":
                return StringUtils.isEmpty(value) ? BigDecimal.ZERO : new BigDecimal(value);
            case "object":
                return value;
            default:
                throw new RuntimeException("Unsupported type " + dataType);
        }
    }

    public static Class<?> convert(String type) {
        int idx = type.indexOf("(");
        if (idx > -1) {
            type = type.substring(0, idx);
        }
        switch (type) {
            case "@int":
                return Integer.TYPE;
            case "@double":
                return Double.TYPE;
            case "@flout":
                return Float.TYPE;
            case "@long":
                return Long.TYPE;
            case "@boolean":
                return Boolean.TYPE;
            case "string":
                return String.class;
            case "byte":
                return Byte.class;
            case "short":
                return Short.class;
            case "integer":
            case "int":
                return Integer.class;
            case "bigint":
            case "long":
                return Long.class;
            case "float":
                return Float.class;
            case "double":
                return Double.class;
            case "bool":
            case "boolean":
                return Boolean.class;
            case "date":
            case "datetype":
                return Date.class;
            case "timestamp":
                return Timestamp.class;
            case "binary":
                return Byte.class;
            case "object":
                return Object.class;
            case "decimal":
                return BigDecimal.class;
            default:
                throw new RuntimeException("Unsupported type " + type);
        }
    }

    public static int toSqlType(String type) {
        int idx = type.indexOf("(");
        if (idx > -1) {
            type = type.substring(0, idx);
        }
        switch (type) {
            case "string":
                return Types.VARCHAR;
            case "byte":
                return Types.TINYINT;
            case "short":
                return Types.SMALLINT;
            case "integer":
            case "int":
                return Types.INTEGER;
            case "bigint":
            case "long":
                return Types.BIGINT;
            case "float":
                return Types.FLOAT;
            case "double":
                return Types.DOUBLE;
            case "bool":
            case "boolean":
                return Types.BOOLEAN;
            case "date":
            case "datetype":
                return Types.DATE;
            case "timestamp":
                return Types.TIMESTAMP;
            case "binary":
                return Types.BINARY;
            case "decimal":
                return Types.DECIMAL;
            case "object":
                return Types.BLOB;
            default:
                throw new RuntimeException("Unsupported type " + type);
        }
    }

    public static boolean isNumber(String typeName) {
        return Arrays.asList(new String[]{"Integer", "Short", "Long", "Float", "Double", "BigDecimal"})
                .contains(typeName);
    }

    public static boolean isEmpty(Object value, String type) {
        if (value == null) {
            return true;
        }
        switch (type.toLowerCase()) {
            case "string":
            case "byte":
                return StringUtils.isEmpty(value.toString());
            case "short":
                return (Short) value == 0;
            case "int":
            case "integer":
                return (Integer) value == 0;
            case "bigint":
            case "long":
                return (Long) value == 0L;
            case "float":
                return (Float) value == 0f;
            case "double":
                return (Double) value == 0.0;
            case "boolean":
                return (Boolean) value == false;
            case "date":
            case "datetype":
            case "timestamp":
            case "binary":
                return value == null;
            case "bigdecimal":
            case "decimal":
                return value.equals(BigDecimal.ZERO);
            default:
                throw new RuntimeException("Not support type " + type);
        }
    }

    public static void main(String[] args) {
//        int i = convert("123", Integer.class);
//        System.out.println(i);
        String str = "decimal(8,7)";

        System.out.println(toSqlType(str));
    }
}
