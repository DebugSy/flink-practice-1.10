package com.flink.demo.cases.case27.datastream.bucket;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.table.utils.PartitionPathUtils;
import org.apache.flink.types.Row;

import java.util.LinkedHashMap;

public class HiveBucketAssigner implements BucketAssigner<Row, String> {

    private final RowTypeInfo rowTypeInfo;

    private final String[] partitionColumns;

    public HiveBucketAssigner(RowTypeInfo rowTypeInfo, String[] partitionColumns) {
        this.rowTypeInfo = rowTypeInfo;
        this.partitionColumns = partitionColumns;
    }

    @Override
    public String getBucketId(Row element, Context context) {
        try {
            LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();
            for (int i = 0; i < partitionColumns.length; i++) {
                String column = partitionColumns[i];
                int index = rowTypeInfo.getFieldIndex(column);
                Object field = element.getField(index);
                String partitionValue = field != null ? field.toString() : null;
                if (StringUtils.isEmpty(partitionValue)) {
                    partitionValue = "__HIVE_DEFAULT_PARTITION__";
                }
                partSpec.put(partitionColumns[i], partitionValue);
            }
            return PartitionPathUtils.generatePartitionPath(partSpec);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
