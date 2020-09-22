package com.flink.demo.cases.case11;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class HBaseSinkFunction
        extends RichSinkFunction<Row>
        implements BufferedMutator.ExceptionListener, CheckpointedFunction {

    private static final long serialVersionUID = 1L;
    private Logger logger = LoggerFactory.getLogger(HBaseSinkFunction.class);

    private int rowKeyIdx;
    private Map<String, String> columnFamilyMap;
    private RowTypeInfo inputRowTypeInfo;
    private String[] inputFieldNames;

    private final long bufferFlushMaxSizeInBytes;
    private final long bufferFlushMaxMutations;

    private transient Connection connection;
    private transient Admin admin;
    private transient Map<String, BufferedMutator> mutators;
    private transient AtomicLong numPendingRequests;

    //metrics
    private transient Counter inputCounter;

    /**
     * This is set from inside the {@link BufferedMutator.ExceptionListener} if a {@link Throwable}
     * was thrown.
     *
     * <p>Errors will be checked and rethrown before processing each input element, and when the sink is closed.
     */
    private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();

    public HBaseSinkFunction(int rowKeyIdx, Map columnFamilyMap, RowTypeInfo inputRowTypeInfo,
                             long bufferFlushMaxSizeInBytes, long bufferFlushMaxMutations) {
        this.inputRowTypeInfo = inputRowTypeInfo;
        this.rowKeyIdx = rowKeyIdx;
        this.columnFamilyMap = columnFamilyMap;
        this.inputFieldNames = inputRowTypeInfo.getFieldNames();
        this.bufferFlushMaxSizeInBytes = bufferFlushMaxSizeInBytes;
        this.bufferFlushMaxMutations = bufferFlushMaxMutations;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        int taskNumber = getRuntimeContext().getIndexOfThisSubtask();
        logger.info("open table, taskNumber = {}", taskNumber);
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        try {
            if (null == connection) {
                this.connection = ConnectionFactory.createConnection(conf);
            }

            this.numPendingRequests = new AtomicLong(0);
        } catch (Throwable e) {
            throw new RuntimeException(String.format("job task %s open table throw exception.",
                    taskNumber), e);
        }

        //register metrics
        MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup("custom_group");
        this.inputCounter = metricGroup.counter("input");
        this.mutators = new HashMap<>();
    }

    @Override
    public void invoke(Row record, Context context) throws Exception {
        inputCounter.inc();

        int taskNumber = getRuntimeContext().getIndexOfThisSubtask();
        String namespace = record.getField(0).toString();
        String tableName = record.getField(1).toString();
        initHbBaseTable(taskNumber, namespace, tableName);
        String fullTableName = namespace + ":" + tableName;
        BufferedMutator mutator;
        if (mutators.containsKey(fullTableName)) {
            mutator = mutators.get(fullTableName);
        } else {
            // create a parameter instance, set the table name and custom listener reference.
            TableName _tableName = TableName.valueOf(fullTableName);
            BufferedMutatorParams params = new BufferedMutatorParams(_tableName)
                    .listener(this)
                    .writeBufferSize(bufferFlushMaxSizeInBytes);
            BufferedMutator _mutator = connection.getBufferedMutator(params);
            mutators.put(fullTableName, _mutator);
            mutator = _mutator;
        }

        Object rowKeyValue = record.getField(rowKeyIdx);
        if (rowKeyValue == null) {
            throw new RuntimeException("The rowkey is empty.");
        }
        String rowKeyType = inputRowTypeInfo.getTypeAt(rowKeyIdx).toString();
        Put put = new Put(convertToBytes(rowKeyType, rowKeyValue));
        for (int i = 2; i < inputFieldNames.length; i++) {
            if (i != rowKeyIdx) {
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

        // flush when the buffer number of mutations greater than the configured max size.
        if (bufferFlushMaxMutations > 0 && numPendingRequests.incrementAndGet() >= bufferFlushMaxMutations) {
            flush();
        }
    }

    @Override
    public void close() throws IOException {
        if (mutators != null && !mutators.isEmpty()) {
            for (BufferedMutator mutator : mutators.values()) {
                try {
                    mutator.close();
                } catch (IOException e) {
                    logger.warn("Exception occurs while closing HBase BufferedMutator.", e);
                }
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                logger.warn("Exception occurs while closing HBase Connection.", e);
            }
            this.connection = null;
        }
    }

    @Override
    public void onException(RetriesExhaustedWithDetailsException exception, BufferedMutator mutator) throws RetriesExhaustedWithDetailsException {
        // fail the sink and skip the rest of the items
        // if the failure handler decides to throw an exception
        failureThrowable.compareAndSet(null, exception);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        while (numPendingRequests != null && numPendingRequests.get() != 0) {
            flush();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
       //nothing to do
    }

    private void flush() throws IOException {
        // BufferedMutator is thread-safe
        for (BufferedMutator mutator : mutators.values()) {
            mutator.flush();
        }
        numPendingRequests.set(0);
        checkErrorAndRethrow();
    }

    private void checkErrorAndRethrow() {
        Throwable cause = failureThrowable.get();
        if (cause != null) {
            throw new RuntimeException("An error occurred in HBaseSink.", cause);
        }
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

    private void initHbBaseTable(int taskNumber, String namespace, String tableName) throws IOException, InterruptedException {
        this.admin = connection.getAdmin();
        boolean isNameSpaceExist = true;
        try {
            admin.getNamespaceDescriptor(namespace);
        } catch (NamespaceNotFoundException e) {
            isNameSpaceExist = false;
        }
        if (!isNameSpaceExist) {
            admin.createNamespace(NamespaceDescriptor.create(namespace).build());
        }

        TableName _tableName = TableName.valueOf(namespace + ":" +tableName);
        if (!admin.isTableAvailable(_tableName)) {
            if (taskNumber == 0) {
                HTableDescriptor desc = new HTableDescriptor(_tableName);
                Set<String> columnFamilys = new HashSet(columnFamilyMap.values());
                for (String cf : columnFamilys) {
                    logger.info("Add family {} to {}", cf, _tableName);
                    desc.addFamily(new HColumnDescriptor(cf));
                }
                admin.createTable(desc);
                logger.info("Created table {} in job task {}", tableName, taskNumber);
            } else {
                while (!admin.isTableAvailable(_tableName)) {
                    logger.info("job task {} waiting for creat table {}", taskNumber, tableName);
                    Thread.sleep(1000);
                }
            }
        }
    }
}