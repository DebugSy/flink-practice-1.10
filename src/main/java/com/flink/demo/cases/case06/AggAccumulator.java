package com.flink.demo.cases.case06;

import lombok.Getter;

/**
 * Created by P0007 on 2019/10/9.
 */
@Getter
public class AggAccumulator {

    private long count;

    public void inc() {
        count++;
    }
}
