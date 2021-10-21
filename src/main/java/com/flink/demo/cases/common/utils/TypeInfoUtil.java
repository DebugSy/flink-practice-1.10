package com.flink.demo.cases.common.utils;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Types;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

/**
 * Created by P0007 on 2019/9/10.
 */
public class TypeInfoUtil {

    /**
     * cast string value to typeInformation
     *
     * @param value
     * @param typeInformation
     * @return
     */
    public static Object cast(String value, TypeInformation typeInformation) {
        switch (typeInformation.toString().toLowerCase()) {
            case "string":
                return value;
            case "byte":
                return Byte.parseByte(value);
            case "short":
                return Short.parseShort(value);
            case "integer":
                return Integer.parseInt(value);
            case "int":
                return Integer.parseInt(value);
            case "bigint":
                return Long.parseLong(value);
            case "long":
                return Long.parseLong(value);
            case "float":
                return Float.parseFloat(value);
            case "double":
                return Double.parseDouble(value);
            case "bool":
                return Boolean.parseBoolean(value);
            case "boolean":
                return Boolean.parseBoolean(value);
            case "date":
                return Date.valueOf(value);
            case "datetype":
                return Date.valueOf(value);
            case "timestamp":
                return Timestamp.valueOf(value);
            case "binary":
                return Byte.parseByte(value);
            case "decimal":
                return BigDecimal.valueOf(Double.parseDouble(value));
            default:
                throw new RuntimeException("Unsupported type " + typeInformation);
        }
    }

}
