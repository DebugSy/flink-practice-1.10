package com.flink.demo.cases.case10.count;

import org.apache.flink.streaming.api.windowing.windows.Window;

public class CountWindow extends Window {

    @Override
    public long maxTimestamp() {
        return Long.MAX_VALUE;
    }

}
