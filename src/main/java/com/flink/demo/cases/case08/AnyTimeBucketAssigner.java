package com.flink.demo.cases.case08;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

/**
 * 支持任意时间的分区，比如10分钟
 */
public class AnyTimeBucketAssigner implements BucketAssigner<Row, String>{

    private static final String DEFAULT_FORMAT_STRING = "yyyy-MM-dd--HH";

    private final String formatString;

    private final ZoneId zoneId;

    private final int timeOffsetMinute;//按照10分钟一个窗口

    private transient DateTimeFormatter dateTimeFormatter;

    public AnyTimeBucketAssigner(int timeOffsetMinute) {
        this(DEFAULT_FORMAT_STRING, timeOffsetMinute);
    }

    public AnyTimeBucketAssigner(String formatString, int timeOffsetMinute) {
        this(formatString, ZoneId.systemDefault(), timeOffsetMinute);
    }

    public AnyTimeBucketAssigner(ZoneId zoneId, int timeOffsetMinute) {
        this(DEFAULT_FORMAT_STRING, zoneId, timeOffsetMinute);
    }

    public AnyTimeBucketAssigner(String formatString, ZoneId zoneId, int timeOffsetMinute) {
        this.formatString = Preconditions.checkNotNull(formatString);
        this.zoneId = Preconditions.checkNotNull(zoneId);
        this.timeOffsetMinute = timeOffsetMinute;
    }

    @Override
    public String getBucketId(Row element, Context context) {
        if (dateTimeFormatter == null) {
            dateTimeFormatter = DateTimeFormatter.ofPattern(formatString).withZone(zoneId);
        }
        Long bucketId = executeBucketId(context.timestamp());
        return dateTimeFormatter.format(Instant.ofEpochMilli(bucketId));
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }

    /**
     * 计算状态清理时间
     *
     * @param timestamp
     * @return
     */
    private Long executeBucketId(Long timestamp) {
        long windowSize = TimeUnit.MINUTES.toMillis(timeOffsetMinute);
        long windowEndTime = timestamp - (timestamp + windowSize) % windowSize + windowSize;
        return windowEndTime;
    }
}
