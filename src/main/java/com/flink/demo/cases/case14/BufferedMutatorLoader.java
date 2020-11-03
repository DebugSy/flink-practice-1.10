package com.flink.demo.cases.case14;

import com.google.common.cache.CacheLoader;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class BufferedMutatorLoader extends CacheLoader<String, BufferedMutator>
        implements BufferedMutator.ExceptionListener{

    private Connection connection;

    private Long bufferFlushMaxSizeInBytes;

    /**
     * This is set from inside the {@link BufferedMutator.ExceptionListener} if a {@link Throwable}
     * was thrown.
     *
     * <p>Errors will be checked and rethrown before processing each input element, and when the sink is closed.
     */
    private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();

    public BufferedMutatorLoader(Long bufferFlushMaxSizeInBytes) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        this.connection = ConnectionFactory.createConnection(conf);
        this.bufferFlushMaxSizeInBytes = bufferFlushMaxSizeInBytes;
    }

    /** 当本地缓存没有时，调用load方法获取结果并将结果缓存 **/
    @Override
    public BufferedMutator load(String table) throws Exception {
        log.debug("loading table {} buffer mutator", table);
        TableName tableName = TableName.valueOf(table);
        BufferedMutatorParams params = new BufferedMutatorParams(tableName)
                .listener(this)
                .writeBufferSize(bufferFlushMaxSizeInBytes);
        return connection.getBufferedMutator(params);
    }

    @Override
    public void onException(RetriesExhaustedWithDetailsException exception, BufferedMutator mutator)
            throws RetriesExhaustedWithDetailsException {
        // fail the sink and skip the rest of the items
        // if the failure handler decides to throw an exception
        failureThrowable.compareAndSet(null, exception);
    }

    public boolean checkErrorAndRethrow() {
        Throwable cause = failureThrowable.get();
        if (cause != null) {
            log.warn("An error occurred in HBaseSink.", cause);
            return true;
        } else {
            return false;
        }
    }
}
