/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flink.demo.cases.case27.batch;

import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.connectors.hive.JobConfWrapper;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.table.functions.hive.FlinkHiveUDFException;
import org.apache.flink.table.functions.hive.conversion.HiveObjectConversion;
import org.apache.flink.table.functions.hive.conversion.IdentityConversion;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

/**
 * Factory for creating {@link RecordWriter} and converters for writing.
 */
public class HiveWriterFactory implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Class hiveOutputFormatClz;

    private final SerDeInfo serDeInfo;

    private final String[] allColumns;

    private final String[] allTypes;

    private final String[] partitionColumns;

    private final Properties tableProperties;

    private final JobConfWrapper confWrapper;

    private final HiveShim hiveShim;

    private final boolean isCompressed;

    // SerDe in Hive-1.2.1 and Hive-2.3.4 can be of different classes, make sure to use a common base class
    private transient Serializer recordSerDe;

    /**
     * Field number excluding partition fields.
     */
    private transient int formatFields;

    // to convert Flink object to Hive object
    private transient HiveObjectConversion[] hiveConversions;

    //StructObjectInspector represents the hive row structure.
    private transient StructObjectInspector formatInspector;

    private transient boolean initialized;

    public HiveWriterFactory(
            JobConf jobConf,
            Class hiveOutputFormatClz,
            SerDeInfo serDeInfo,
            String[] allColumns,
            String[] allTypes,
            String[] partitionColumns,
            Properties tableProperties,
            HiveShim hiveShim,
            boolean isCompressed) {
        Preconditions.checkArgument(HiveOutputFormat.class.isAssignableFrom(hiveOutputFormatClz),
                "The output format should be an instance of HiveOutputFormat");
        this.confWrapper = new JobConfWrapper(jobConf);
        this.hiveOutputFormatClz = hiveOutputFormatClz;
        this.serDeInfo = serDeInfo;
        this.allColumns = allColumns;
        this.allTypes = allTypes;
        this.partitionColumns = partitionColumns;
        this.tableProperties = tableProperties;
        this.hiveShim = hiveShim;
        this.isCompressed = isCompressed;
    }

    /**
     * Create a {@link RecordWriter} from path.
     */
    public RecordWriter createRecordWriter(Path path) {
        try {
            checkInitialize();
            JobConf conf = new JobConf(confWrapper.conf());

            if (isCompressed) {
                String codecStr = conf.get(HiveConf.ConfVars.COMPRESSINTERMEDIATECODEC.varname);
                if (!StringUtils.isNullOrWhitespaceOnly(codecStr)) {
                    //noinspection unchecked
                    Class<? extends CompressionCodec> codec =
                            (Class<? extends CompressionCodec>) Class.forName(codecStr, true,
                                    Thread.currentThread().getContextClassLoader());
                    FileOutputFormat.setOutputCompressorClass(conf, codec);
                }
                String typeStr = conf.get(HiveConf.ConfVars.COMPRESSINTERMEDIATETYPE.varname);
                if (!StringUtils.isNullOrWhitespaceOnly(typeStr)) {
                    SequenceFile.CompressionType style = SequenceFile.CompressionType.valueOf(typeStr);
                    SequenceFileOutputFormat.setOutputCompressionType(conf, style);
                }
            }

            return hiveShim.getHiveRecordWriter(
                    conf,
                    hiveOutputFormatClz,
                    recordSerDe.getSerializedClass(),
                    isCompressed,
                    tableProperties,
                    path);
        } catch (Exception e) {
            throw new FlinkHiveException(e);
        }
    }

    public JobConf getJobConf() {
        return confWrapper.conf();
    }

    private void checkInitialize() throws Exception {
        if (initialized) {
            return;
        }

        JobConf jobConf = confWrapper.conf();
        Object serdeLib = Class.forName(serDeInfo.getSerializationLib()).newInstance();
        Preconditions.checkArgument(serdeLib instanceof Serializer && serdeLib instanceof Deserializer,
                "Expect a SerDe lib implementing both Serializer and Deserializer, but actually got "
                        + serdeLib.getClass().getName());
        this.recordSerDe = (Serializer) serdeLib;
        ReflectionUtils.setConf(recordSerDe, jobConf);

        // TODO: support partition properties, for now assume they're same as table properties
        SerDeUtils.initializeSerDe((Deserializer) recordSerDe, jobConf, tableProperties, null);

        this.formatFields = allColumns.length - partitionColumns.length;
        this.hiveConversions = new HiveObjectConversion[formatFields];
        List<ObjectInspector> objectInspectors = new ArrayList<>(hiveConversions.length);
        for (int i = 0; i < formatFields; i++) {
            String type = allTypes[i];
            PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) toHiveTypeInfo(type);
            ObjectInspector objectInspector = getObjectInspector(primitiveTypeInfo);
            objectInspectors.add(objectInspector);
            hiveConversions[i] = getConversion(objectInspector, hiveShim);
        }

        this.formatInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
                Arrays.asList(allColumns).subList(0, formatFields),
                objectInspectors);
        this.initialized = true;
    }

    public Function<Row, Writable> createRowConverter() {
        return row -> {
            List<Object> fields = new ArrayList<>(formatFields);
            for (int i = 0; i < formatFields; i++) {
                fields.add(hiveConversions[i].toHiveObject(row.getField(i)));
            }
            return serialize(fields);
        };
    }

    private Writable serialize(List<Object> fields) {
        try {
            return recordSerDe.serialize(fields, formatInspector);
        } catch (SerDeException e) {
            throw new FlinkHiveException(e);
        }
    }

    /**
     * Get conversion for converting Flink object to Hive object from an ObjectInspector and the corresponding Flink DataType.
     */
    public static HiveObjectConversion getConversion(ObjectInspector inspector, HiveShim hiveShim) {
        if (inspector instanceof PrimitiveObjectInspector) {
            HiveObjectConversion conversion;
            if (inspector instanceof BooleanObjectInspector ||
                    inspector instanceof StringObjectInspector ||
                    inspector instanceof ByteObjectInspector ||
                    inspector instanceof ShortObjectInspector ||
                    inspector instanceof IntObjectInspector ||
                    inspector instanceof LongObjectInspector ||
                    inspector instanceof FloatObjectInspector ||
                    inspector instanceof DoubleObjectInspector ||
                    inspector instanceof BinaryObjectInspector) {
                conversion = IdentityConversion.INSTANCE;
            } else if (inspector instanceof DateObjectInspector) {
                conversion = hiveShim::toHiveDate;
            } else if (inspector instanceof TimestampObjectInspector) {
                conversion = hiveShim::toHiveTimestamp;
            } else if (inspector instanceof HiveCharObjectInspector) {
                conversion = o -> o == null ? null : new HiveChar((String) o, ((String) o).length());
            } else if (inspector instanceof HiveVarcharObjectInspector) {
                conversion = o -> o == null ? null : new HiveVarchar((String) o, ((String) o).length());
            } else if (inspector instanceof HiveDecimalObjectInspector) {
                conversion = o -> o == null ? null : HiveDecimal.create((BigDecimal) o);
            } else {
                throw new FlinkHiveUDFException("Unsupported primitive object inspector " + inspector.getClass().getName());
            }
            return conversion;
        }

        throw new FlinkHiveUDFException(
                String.format("Flink doesn't support convert object conversion for %s yet", inspector));
    }

    private static ObjectInspector getObjectInspectorForPrimitiveConstant(
            PrimitiveTypeInfo primitiveTypeInfo, @Nonnull Object value, HiveShim hiveShim) {
        String className;
        value = hiveShim.hivePrimitiveToWritable(value);
        switch (primitiveTypeInfo.getPrimitiveCategory()) {
            case BOOLEAN:
                className = WritableConstantBooleanObjectInspector.class.getName();
                return HiveReflectionUtils.createConstantObjectInspector(className, value);
            case BYTE:
                className = WritableConstantByteObjectInspector.class.getName();
                return HiveReflectionUtils.createConstantObjectInspector(className, value);
            case SHORT:
                className = WritableConstantShortObjectInspector.class.getName();
                return HiveReflectionUtils.createConstantObjectInspector(className, value);
            case INT:
                className = WritableConstantIntObjectInspector.class.getName();
                return HiveReflectionUtils.createConstantObjectInspector(className, value);
            case LONG:
                className = WritableConstantLongObjectInspector.class.getName();
                return HiveReflectionUtils.createConstantObjectInspector(className, value);
            case FLOAT:
                className = WritableConstantFloatObjectInspector.class.getName();
                return HiveReflectionUtils.createConstantObjectInspector(className, value);
            case DOUBLE:
                className = WritableConstantDoubleObjectInspector.class.getName();
                return HiveReflectionUtils.createConstantObjectInspector(className, value);
            case STRING:
                className = WritableConstantStringObjectInspector.class.getName();
                return HiveReflectionUtils.createConstantObjectInspector(className, value);
            case CHAR:
                try {
                    Constructor<WritableConstantHiveCharObjectInspector> constructor =
                            WritableConstantHiveCharObjectInspector.class.getDeclaredConstructor(CharTypeInfo.class, value.getClass());
                    constructor.setAccessible(true);
                    return constructor.newInstance(primitiveTypeInfo, value);
                } catch (Exception e) {
                    throw new FlinkHiveUDFException("Failed to create writable constant object inspector", e);
                }
            case VARCHAR:
                try {
                    Constructor<WritableConstantHiveVarcharObjectInspector> constructor =
                            WritableConstantHiveVarcharObjectInspector.class.getDeclaredConstructor(VarcharTypeInfo.class, value.getClass());
                    constructor.setAccessible(true);
                    return constructor.newInstance(primitiveTypeInfo, value);
                } catch (Exception e) {
                    throw new FlinkHiveUDFException("Failed to create writable constant object inspector", e);
                }
            case DATE:
                className = WritableConstantDateObjectInspector.class.getName();
                return HiveReflectionUtils.createConstantObjectInspector(className, value);
            case TIMESTAMP:
                className = WritableConstantTimestampObjectInspector.class.getName();
                return HiveReflectionUtils.createConstantObjectInspector(className, value);
            case DECIMAL:
                try {
                    Constructor<WritableConstantHiveDecimalObjectInspector> constructor =
                            WritableConstantHiveDecimalObjectInspector.class.getDeclaredConstructor(DecimalTypeInfo.class, value.getClass());
                    constructor.setAccessible(true);
                    return constructor.newInstance(primitiveTypeInfo, value);
                } catch (Exception e) {
                    throw new FlinkHiveUDFException("Failed to create writable constant object inspector", e);
                }
            case BINARY:
                className = WritableConstantBinaryObjectInspector.class.getName();
                return HiveReflectionUtils.createConstantObjectInspector(className, value);
            case UNKNOWN:
            case VOID:
                // If type is null, we use the Constant String to replace
                className = WritableConstantStringObjectInspector.class.getName();
                return HiveReflectionUtils.createConstantObjectInspector(className, value.toString());
            default:
                throw new FlinkHiveUDFException(
                        String.format("Cannot find ConstantObjectInspector for %s", primitiveTypeInfo));
        }
    }

    private static ObjectInspector getObjectInspector(TypeInfo type) {
        switch (type.getCategory()) {

            case PRIMITIVE:
                PrimitiveTypeInfo primitiveType = (PrimitiveTypeInfo) type;
                return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(primitiveType);
            case LIST:
                ListTypeInfo listType = (ListTypeInfo) type;
                return ObjectInspectorFactory.getStandardListObjectInspector(
                        getObjectInspector(listType.getListElementTypeInfo()));
            case MAP:
                MapTypeInfo mapType = (MapTypeInfo) type;
                return ObjectInspectorFactory.getStandardMapObjectInspector(
                        getObjectInspector(mapType.getMapKeyTypeInfo()), getObjectInspector(mapType.getMapValueTypeInfo()));
            case STRUCT:
                StructTypeInfo structType = (StructTypeInfo) type;
                List<TypeInfo> fieldTypes = structType.getAllStructFieldTypeInfos();

                List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>();
                for (TypeInfo fieldType : fieldTypes) {
                    fieldInspectors.add(getObjectInspector(fieldType));
                }

                return ObjectInspectorFactory.getStandardStructObjectInspector(
                        structType.getAllStructFieldNames(), fieldInspectors);
            default:
                throw new CatalogException("Unsupported Hive type category " + type.getCategory());
        }
    }

    // TODO string 和 decimal
    private TypeInfo toHiveTypeInfo(String dataType) {
        switch (dataType) {
            case "string":
                return TypeInfoFactory.getVarcharTypeInfo(255);
            case "boolean":
                return TypeInfoFactory.booleanTypeInfo;
            case "decimal":
                return TypeInfoFactory.getDecimalTypeInfo(38, 18);
            case "byte":
                return TypeInfoFactory.byteTypeInfo;
            case "short":
                return TypeInfoFactory.shortTypeInfo;
            case "date":
                return TypeInfoFactory.dateTypeInfo;
            case "int":
                return TypeInfoFactory.intTypeInfo;
            case "bigint":
                return TypeInfoFactory.longTypeInfo;
            case "float":
                return TypeInfoFactory.floatTypeInfo;
            case "double":
                return TypeInfoFactory.doubleTypeInfo;
            case "timestamp":
                return TypeInfoFactory.timestampTypeInfo;
            default:
                throw new RuntimeException("Unsupported type " + dataType);
        }
    }
}
