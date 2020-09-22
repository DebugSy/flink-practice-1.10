package com.flink.demo.cases.case08;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.File;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class EventTimeBucketAssigner implements BucketAssigner<Row, String>{

    private static final String DEFAULT_FORMAT_STRING = "yyyy-MM-dd--HH";

    private final String formatString;

    private final ZoneId zoneId;

    private transient DateTimeFormatter dateTimeFormatter;

    public EventTimeBucketAssigner() {
        this(DEFAULT_FORMAT_STRING);
    }

    public EventTimeBucketAssigner(String formatString) {
        this(formatString, ZoneId.systemDefault());
    }

    public EventTimeBucketAssigner(ZoneId zoneId) {
        this(DEFAULT_FORMAT_STRING, zoneId);
    }

    public EventTimeBucketAssigner(String formatString, ZoneId zoneId) {
        this.formatString = Preconditions.checkNotNull(formatString);
        this.zoneId = Preconditions.checkNotNull(zoneId);
    }

    @Override
    public String getBucketId(Row element, Context context) {
        if (dateTimeFormatter == null) {
            dateTimeFormatter = DateTimeFormatter.ofPattern(formatString).withZone(zoneId);
        }
        return dateTimeFormatter.format(Instant.ofEpochMilli(context.timestamp()));
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
