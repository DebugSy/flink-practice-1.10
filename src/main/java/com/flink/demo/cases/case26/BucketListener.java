package com.flink.demo.cases.case26;

import org.apache.flink.core.fs.Path;

import java.io.Serializable;

public interface BucketListener<BucketId> extends Serializable {

    void notifyBucketInactive(BucketInfo<BucketId> bucketInfo);

}
