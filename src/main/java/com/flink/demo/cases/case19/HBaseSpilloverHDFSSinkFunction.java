package com.flink.demo.cases.case19;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by P0007 on 2020/10/28.
 *
 * 自定义HBase Sink
 * 1. 数据写入HBase,失败则外溢到HDFS
 */
@Slf4j
public class HBaseSpilloverHDFSSinkFunction extends RichSinkFunction<Row>
        implements BufferedMutator.ExceptionListener, CheckpointedFunction {

    private String tableName = "shiy-flink-hbase-sink";
    private int tableColumnIdx;
    private int rowKeyIdx;
    private Map<String, String> columnFamilyMap;
    private RowTypeInfo inputRowTypeInfo;
    private String[] inputFieldNames;

    private final long bufferFlushMaxSizeInBytes;
    private final long bufferFlushMaxMutations;

    private transient Connection connection;
    private transient BufferedMutator mutator;

    private ListState<Row> checkpointState;
    private List<Row> bufferRecords;

    //metrics
    private transient Counter inputCounter;

    private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();

    /**
     *
     * @param tableColumn 代表HBase table name的列
     * @param rowKeyIdx HBase rowkey字段的下标
     * @param columnFamilyMap HBase列簇描述
     * @param inputRowTypeInfo 输入HBase数据类型
     * @param bufferFlushMaxSizeInBytes HBase BufferedMutator缓存数据大小
     * @param bufferFlushMaxMutations HBase BufferedMutator缓存数据条目
     */
    public HBaseSpilloverHDFSSinkFunction(String tableColumn, int rowKeyIdx, Map columnFamilyMap, RowTypeInfo inputRowTypeInfo,
                                          long bufferFlushMaxSizeInBytes, long bufferFlushMaxMutations) {
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
        this.bufferRecords = new ArrayList<>();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        int taskNumber = getRuntimeContext().getIndexOfThisSubtask();
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        try {
            if (null == connection) {
                this.connection = ConnectionFactory.createConnection(conf);
            }

            // create a parameter instance, set the table name and custom listener reference.
            BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName))
                    .listener(this)
                    .writeBufferSize(bufferFlushMaxSizeInBytes);
            this.mutator = connection.getBufferedMutator(params);

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
        bufferRecords.add(record);
        // flush when the buffer number of mutations greater than the configured max size.
        if (bufferFlushMaxMutations > 0 && bufferRecords.size() >= bufferFlushMaxMutations) {
            flush();
            bufferRecords.clear();
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
        checkpointState.clear();
        Iterator<Row> iterator = bufferRecords.iterator();
        while (iterator.hasNext()) {
            Row record = iterator.next();
            checkpointState.add(record);
        }
        log.debug("finished flush all mutator cache on snapshot.");
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Row> cacheDesc = new ListStateDescriptor<>("sink_cache", TypeInformation.of(Row.class));
        this.checkpointState = context.getOperatorStateStore().getListState(cacheDesc);
        if (context.isRestored()) {
            for (Row record : checkpointState.get()) {
                bufferRecords.add(record);
            }
        }
    }

    private void flush() throws Exception {
        // BufferedMutator is thread-safe
        log.debug("flush mutator cache for table {}", tableName);
        for (Row record : bufferRecords) {
            createPutAction(record);
        }
        mutator.flush();
        checkpointState.clear();
        checkErrorAndRethrow();
    }

    private void createPutAction(Row record) throws Exception {
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
        mutator.mutate(put);
    }

    @Override
    public void onException(RetriesExhaustedWithDetailsException exception, BufferedMutator mutator) throws RetriesExhaustedWithDetailsException {
        // fail the sink and skip the rest of the items
        // if the failure handler decides to throw an exception
        failureThrowable.compareAndSet(null, exception);
        log.error("spillover data to hdfs ......");
        for (Row record : bufferRecords) {
            log.error("{}", record);
        }
        bufferRecords.clear();
        log.error("finish spillover.");
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

    private void checkErrorAndRethrow() {
        Throwable cause = failureThrowable.get();
        if (cause != null) {
            throw new RuntimeException("An error occurred in HBaseSink.", cause);
        }
    }
}