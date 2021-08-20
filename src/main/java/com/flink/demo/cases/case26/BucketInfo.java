package com.flink.demo.cases.case26;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.core.fs.Path;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BucketInfo<BucketId> {

    private BucketId bucketId;

    private Path path;

    private long records;

    private long fileLength;

    private long fileCount;

}
