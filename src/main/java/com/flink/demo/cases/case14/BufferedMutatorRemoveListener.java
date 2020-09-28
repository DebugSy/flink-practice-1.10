package com.flink.demo.cases.case14;

import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.BufferedMutator;

import java.io.IOException;
import java.io.Serializable;

@Slf4j
public class BufferedMutatorRemoveListener implements RemovalListener<String, BufferedMutator>, Serializable {

    @Override
    public void onRemoval(RemovalNotification<String, BufferedMutator> removalNotification) {
        /**
         * 当缓存超过一定时间没访问的时候，从缓存移除并关闭表连接
         */
        String table = removalNotification.getKey();
        log.debug("closing mutator for table {}", table);
        BufferedMutator mutator = removalNotification.getValue();
        try {
            mutator.flush();
            mutator.close();
        } catch (IOException e) {
            log.error("closing HBase table {} mutator throw exception", table);
        }
    }
}
