package com.flink.demo.cases.case27.datastream.parquet;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DecimalContainer {

    public static final int MIN_PRECISION = 1;
    public static final int MAX_PRECISION = 38;
    public static final int DEFAULT_PRECISION = 10;
    public static final int MIN_SCALE = 0;
    public static final int DEFAULT_SCALE = 0;
    public static final int MAX_COMPACT_PRECISION = 18;

    /**
     * Maximum number of decimal digits an Int can represent. (1e9 < Int.MaxValue < 1e10)
     */
    public static final int MAX_INT_DIGITS = 9;

    /**
     * Maximum number of decimal digits a Long can represent. (1e18 < Long.MaxValue < 1e19)
     */
    public static final int MAX_LONG_DIGITS = 18;

    private long decimalLongValue;
    private int precision;
    private int scale;

    public DecimalContainer(int precision, int scale) {
        this.precision = precision;
        this.scale = scale;
    }
}
