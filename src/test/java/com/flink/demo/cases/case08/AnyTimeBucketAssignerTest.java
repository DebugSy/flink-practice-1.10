package com.flink.demo.cases.case08;

import junit.framework.TestCase;

import java.time.*;
import java.util.concurrent.TimeUnit;

public class AnyTimeBucketAssignerTest extends TestCase {

    public void testParseTime() {
        LocalDateTime localDateTime = LocalDateTime.parse("2021-09-22T16:36:21.632");
        System.out.println(localDateTime);
        long milli = localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        long windowStartWithOffset = getWindowEnd(milli, TimeUnit.MINUTES.toMillis(5));
        LocalDateTime result = LocalDateTime.ofInstant(Instant.ofEpochMilli(windowStartWithOffset), ZoneId.systemDefault());
        System.err.println(result);
    }

    private long getWindowEnd(long timestamp, long windowSize) {
        return timestamp - (timestamp + windowSize) % windowSize + windowSize;
    }

}