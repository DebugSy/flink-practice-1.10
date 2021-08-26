package com.flink.demo.cases.case26;

import java.io.Serializable;

public interface BucketListener<BucketId> extends Serializable {

    void notifyBucketInactive(BucketInfo<BucketId> bucketInfo);

}
