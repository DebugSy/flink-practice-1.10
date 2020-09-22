package com.flink.demo.cases.common.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

/**
 * Created by joey on 2017/3/21.
 */
public final class ConvertUtil {

    private static final Logger logger = LoggerFactory.getLogger(ConvertUtil.class);
    static int wrongCount = 0;

    public static long toTimestamp(Object obj, String pattern, long defaultValue) {
        if (obj instanceof Long) {
            return (Long) obj;
        } else if (obj instanceof Timestamp) {
            return ((Timestamp) obj).getTime();
        } else if (obj instanceof String) {
            String str = (String) obj;
            try {
                DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern(pattern.trim());
                return dateTimeFormatter.parseMillis(str);
            } catch (Exception e) {
                wrongCount++;
                logger.warn("Time column value '{}' can't match pattern '{}',please check...", str, pattern);
                if (wrongCount >= 10000) {
                    throw new RuntimeException("Time column can't match pattern '" + pattern
                            + "' more than 10000 times! like value: " + str);
                }
                return defaultValue;
            }
        }
        return defaultValue;
    }

    public static TypeInformation<?>[] toTypeInformations(String[] dataTypes) {
        TypeInformation<?>[] typeInformations = new TypeInformation[dataTypes.length];
        for (int i = 0; i < dataTypes.length; i++) {
            typeInformations[i] = TypeExtractor.getForClass(convert(dataTypes[i]));
        }
        return typeInformations;
    }

    /**
     * 将String类型转换为指定类型
     *
     * @param dataType
     * @return object
     */
    public static Object castNull(String dataType) {
        if (dataType.contains("(")) {
            dataType = dataType.substring(0, dataType.indexOf("("));
        }
        switch (dataType.toLowerCase()) {
            case "string":
                return "";
            case "byte":
                return "";
            case "short":
                return null;
            case "integer":
                return null;
            case "int":
                return null;
            case "bigint":
                return null;
            case "long":
                return null;
            case "float":
                return null;
            case "double":
                return null;
            case "bool":
                return null;
            case "boolean":
                return null;
            case "date":
                return null;
            case "datetype":
                return null;
            case "timestamp":
                return null;
            case "binary":
                return "";
            case "decimal":
                return null;
            default:
                throw new RuntimeException("Unsupported type " + dataType);
        }
    }

    public static Object convert(String value, String dataType) {
        logger.debug("Convert data[{}] to type[{}]", value, dataType);
        if (dataType.contains("(")) {
            dataType = dataType.substring(0, dataType.indexOf("("));
        }
        switch (dataType.toLowerCase()) {
            case "string":
                return StringUtils.isEmpty(value) ? "" : value.trim();
            case "byte":
                return StringUtils.isEmpty(value) ? "" : Byte.parseByte(value);
            case "short":
                return StringUtils.isEmpty(value) ? 0 : Short.parseShort(value);
            case "integer":
                return StringUtils.isEmpty(value) ? 0 : Integer.parseInt(value);
            case "int":
                return StringUtils.isEmpty(value) ? 0 : Integer.parseInt(value);
            case "bigint":
                return StringUtils.isEmpty(value) ? 0l : Long.parseLong(value);
            case "long":
                return StringUtils.isEmpty(value) ? 0l : Long.parseLong(value);
            case "float":
                return StringUtils.isEmpty(value) ? 0f : Float.parseFloat(value);
            case "double":
                return StringUtils.isEmpty(value) ? 0.0 : Double.parseDouble(value);
            case "bool":
                return StringUtils.isEmpty(value) ? false : Boolean.parseBoolean(value);
            case "boolean":
                return StringUtils.isEmpty(value) ? false : Boolean.parseBoolean(value);
            case "date":
                return StringUtils.isEmpty(value) ? null : Date.valueOf(value);
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
        if (type.contains("(")) {
            type = type.substring(0, type.indexOf("("));
        }
        switch (type.toLowerCase()) {
            case "string":
                return String.class;
            case "tinyint":
            case "byte":
                return Byte.class;
            case "smallint":
            case "short":
                return Short.class;
            case "integer":
                return Integer.class;
            case "int":
                return Integer.class;
            case "bigint":
                return Long.class;
            case "long":
                return Long.class;
            case "float":
                return Float.class;
            case "double":
                return Double.class;
            case "bool":
                return Boolean.class;
            case "boolean":
                return Boolean.class;
            case "date":
                return Date.class;
            case "datetype":
                return Date.class;
            case "timestamp":
                return Timestamp.class;
            case "decimal":
            case "legacy":
                return BigDecimal.class;
            case "object":
                return Object.class;
            default:
                throw new RuntimeException("Unsupported type " + type);
        }
    }

    public static Object convert(String value, Class<?> targetClass) {
        return convert(value, targetClass.getSimpleName());
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

    public static void main(String[] args) {
        System.out.println(convert("0", Integer.class));
    }
}
