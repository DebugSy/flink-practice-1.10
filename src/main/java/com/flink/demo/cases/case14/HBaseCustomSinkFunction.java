package com.flink.demo.cases.case14;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by P0007 on 2020/9/24.
 *
 * 自定义HBase Sink
 * 1. 动态切换HBase Table,目前是从数据中抽取代表tableColumn的列作为HBase Table Name
 * 2. 利用HBase Client @{@link BufferedMutator}缓存写入HBase的数据
 * 3. Guava Cache缓存@{@link BufferedMutator},长时间没有访问该Table,会触发清理(flush和close)
 */
@Slf4j
public class HBaseCustomSinkFunction extends RichSinkFunction<Row> implements CheckpointedFunction {

    private int tableColumnIdx;
    private int rowKeyIdx;
    private Map<String, String> columnFamilyMap;
    private RowTypeInfo inputRowTypeInfo;
    private String[] inputFieldNames;
    private long cacheExpireSecond;

    private final long bufferFlushMaxSizeInBytes;
    private final long bufferFlushMaxMutations;

    private transient Connection connection;
    private transient BufferedMutatorLoader mutatorLoader;
    private transient LoadingCache<String, BufferedMutator> mutators;
    private transient AtomicLong numPendingRequests;

    //metrics
    private transient Counter inputCounter;

    /**
     *
     * @param tableColumn 代表HBase table name的列
     * @param rowKeyIdx HBase rowkey字段的下标
     * @param columnFamilyMap HBase列簇描述
     * @param inputRowTypeInfo 输入HBase数据类型
     * @param bufferFlushMaxSizeInBytes HBase BufferedMutator缓存数据大小
     * @param bufferFlushMaxMutations HBase BufferedMutator缓存数据条目
     * @param cacheExpireSecond BufferedMutator超时时间(秒)
     */
    public HBaseCustomSinkFunction(String tableColumn, int rowKeyIdx, Map columnFamilyMap, RowTypeInfo inputRowTypeInfo,
                                   long bufferFlushMaxSizeInBytes, long bufferFlushMaxMutations, long cacheExpireSecond) {
        this.inputRowTypeInfo = inputRowTypeInfo;
        int fieldIndex = inputRowTypeInfo.getFieldIndex(tableColumn);
        if (fieldIndex == -1) {
            throw new RuntimeException("table column " + tableColumn + " is not exist in " + inputRowTypeInfo);
        }
        this.tableColumnIdx = fieldIndex;
        this.rowKeyIdx = rowKeyIdx;
        this.columnFamilyMap = columnFamilyMap;
        this.inputFieldNames = inputRowTypeInfo.getFieldNames();
        this.bufferFlushMaxSizeInBytes = bufferFlushMaxSizeInBytes;
        this.bufferFlushMaxMutations = bufferFlushMaxMutations;
        this.cacheExpireSecond = cacheExpireSecond;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        int taskNumber = getRuntimeContext().getIndexOfThisSubtask();
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        try {
            if (null == connection) {
                this.connection = ConnectionFactory.createConnection(conf);
            }

            this.numPendingRequests = new AtomicLong(0);
            this.mutatorLoader = new BufferedMutatorLoader(bufferFlushMaxSizeInBytes);
            this.mutators = CacheBuilder
                    .newBuilder()
                    .expireAfterAccess(cacheExpireSecond, TimeUnit.SECONDS) // 设置多久没访问就从缓存清理掉
                    .removalListener(new BufferedMutatorRemoveListener())
                    .build(mutatorLoader);

        } catch (Throwable e) {
            throw new RuntimeException(String.format("job task %s open connection throw exception.", taskNumber), e);
        }

        //register metrics
        MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup("custom_group");
        this.inputCounter = metricGroup.counter("input");
    }

    @Override
    public void invoke(Row record, Context context) throws Exception {
        inputCounter.inc();
        log.debug("input data {}", record);
        Object tableNameV = record.getField(tableColumnIdx);
        if (tableNameV == null) {
            throw new RuntimeException("The table name is empty.");
        }
        String tableName = tableNameV.toString();
        Object rowKeyValue = record.getField(rowKeyIdx);
        if (rowKeyValue == null) {
            throw new RuntimeException("The rowkey is empty.");
        }
        String rowKeyType = inputRowTypeInfo.getTypeAt(rowKeyIdx).toString();
        Put put = new Put(convertToBytes(rowKeyType, rowKeyValue));
        for (int i = 0; i < inputFieldNames.length; i++) {
            if (i != rowKeyIdx && i != tableColumnIdx) {
                String field = inputFieldNames[i];
                String columnFamily = columnFamilyMap.get(field);
                if (StringUtils.isEmpty(columnFamily)) {
                    throw new RuntimeException("Not found column family for field " + field);
                }
                int index = inputRowTypeInfo.getFieldIndex(field);
                String type = inputRowTypeInfo.getTypeAt(field).toString();
                Object obj = record.getField(index);
                byte[] value;
                if (obj != null) {
                    value = convertToBytes(type, obj);
                } else {
                    value = "".getBytes();
                }
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(field), value);
            }
        }
        BufferedMutator mutator = mutators.get(tableName);
        mutator.mutate(put);

        // flush when the buffer number of mutations greater than the configured max size.
        if (bufferFlushMaxMutations > 0 && numPendingRequests.incrementAndGet() >= bufferFlushMaxMutations) {
            flush(tableName);
        }
    }


    @Override
    public void close() throws IOException {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                log.warn("Exception occurs while closing HBase Connection.", e);
            }
            this.connection = null;
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        log.debug("starting flush all mutator cache on snapshot......");
        while (numPendingRequests != null && numPendingRequests.get() != 0) {
            ConcurrentMap<String, BufferedMutator> mutators = this.mutators.asMap();
            for (Map.Entry<String, BufferedMutator> entry : mutators.entrySet()) {
                BufferedMutator mutator = entry.getValue();
                mutator.flush();
                numPendingRequests.set(0);
                mutatorLoader.checkErrorAndRethrow();
            }
        }
        log.debug("finished flush all mutator cache on snapshot.");
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // nothing to do.
    }

    private void flush(String tableName) throws Exception {
        // BufferedMutator is thread-safe
        log.debug("flush mutator cache for table {}", tableName);
        BufferedMutator mutator = mutators.get(tableName);
        mutator.flush();
        numPendingRequests.set(0);
        mutatorLoader.checkErrorAndRethrow();
    }

    /**
     * 与Spark HBase Connector保持一致，flink输出到HBase后，需要spark对接可读
     * 基本类型支持，非基本类型需要转换
     * 参考 org.apache.spark.sql.execution.datasources.hbase.types.PrimitiveType#toBytes(java.lang.Object)
     *
     * @param type
     * @return
     */
    private byte[] convertToBytes(String type, Object value) {
        int idx = type.indexOf("(");
        String subStrType = type;
        if (idx > -1) {
            subStrType = type.substring(0, idx);
        }
        switch (subStrType.toLowerCase()) {
            case "string":
                return Bytes.toBytes((String) value);
            case "byte":
                return Bytes.toBytes((Byte) value);
            case "short":
                return Bytes.toBytes((Short) value);
            case "int":
            case "integer":
                return Bytes.toBytes((Integer) value);
            case "bigint":
            case "long":
                return Bytes.toBytes((Long) value);
            case "float":
                return Bytes.toBytes((Float) value);
            case "double":
                return Bytes.toBytes((Double) value);
            case "boolean":
                return Bytes.toBytes((Boolean) value);
            case "date":
            case "datetype":
                Date date = (Date) value;
                return Bytes.toBytes(date.getTime());
            case "timestamp":
                Timestamp timestamp = (Timestamp) value;
                return Bytes.toBytes(timestamp.getTime());
            case "bigdecimal":
            case "decimal":
                BigDecimal decimal = (BigDecimal) value;
                return convertDecimal(type, decimal);
            default:
                throw new RuntimeException("Not support type " + type);
        }
    }

    private byte[] convertDecimal(String decimalFieldType, BigDecimal decimal){
        String typeInJson = decimalFieldType;
        int begin = typeInJson.indexOf("(");
        int end = typeInJson.indexOf(")");
        int sep = typeInJson.indexOf(",");
        if(begin>0 && end>0 && sep > 0){
            int precision = Integer.parseInt(typeInJson.substring(begin + 1, sep).trim());
            int scale = Integer.parseInt(typeInJson.substring(sep + 1, end).trim());

            if (scale == 0) {
                if (precision < 11) {
                    return Bytes.toBytes(decimal.intValue());
                } else{
                    return Bytes.toBytes(decimal.longValue());
                }
            } else {
                return Bytes.toBytes(decimal.doubleValue());
            }
        } else {
            return Bytes.toBytes(decimal.doubleValue());
        }
    }
}