package com.flink.demo.cases.case13;

import org.apache.flink.api.common.serialization.SimpleStringSchema;

public class LimitDeserializationSchema extends SimpleStringSchema {

    private long count = 0;

    @Override
    public boolean isEndOfStream(String nextElement) {
        count++;
        if (count == 10) {
            return true;
        }
        return false;
    }
}
