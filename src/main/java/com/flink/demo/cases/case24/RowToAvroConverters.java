/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flink.demo.cases.case24;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.data.*;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.*;

/**
 * Tool class used to convert from {@link RowData} to Avro {@link GenericRecord}.
 *
 * <p>NOTE: reference from Flink release 1.12.0, should remove when Flink version upgrade to that.
 */
@Internal
public class RowToAvroConverters {

    // --------------------------------------------------------------------------------
    // Runtime Converters
    // --------------------------------------------------------------------------------

    /**
     * Runtime converter that converts objects of Flink Table & SQL internal data structures to
     * corresponding Avro data structures.
     */
    @FunctionalInterface
    public interface RowDataToAvroConverter extends Serializable {
        Object convert(Schema schema, Object object);
    }

    // --------------------------------------------------------------------------------
    // IMPORTANT! We use anonymous classes instead of lambdas for a reason here. It is
    // necessary because the maven shade plugin cannot relocate classes in
    // SerializedLambdas (MSHADE-260). On the other hand we want to relocate Avro for
    // sql-client uber jars.
    // --------------------------------------------------------------------------------

    /**
     * Creates a runtime converter accroding to the given logical type that converts objects of
     * Flink Table & SQL internal data structures to corresponding Avro data structures.
     */
    public static RowDataToAvroConverter createConverter(TypeInformation<?> type) {
        final RowDataToAvroConverter converter;
        if (type.equals(Types.VOID)) {
            converter =
                    new RowToAvroConverters.RowDataToAvroConverter() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public Object convert(Schema schema, Object object) {
                            return null;
                        }
                    };
        } else if (type.equals(Types.SHORT)) {
            converter =
                    new RowDataToAvroConverter() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public Object convert(Schema schema, Object object) {
                            return ((Byte) object).intValue();
                        }
                    };
        } else if (type.equals(Types.BYTE)) {
            converter =
                    new RowDataToAvroConverter() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public Object convert(Schema schema, Object object) {
                            return ((Short) object).intValue();
                        }
                    };
        } else if (type.equals(Types.BOOLEAN)
                || type.equals(Types.INT)
                || type.equals(Types.LONG)
                || type.equals(Types.BIG_INT)
                || type.equals(Types.FLOAT)
                || type.equals(Types.DOUBLE)
                || type.equals(Types.SQL_DATE)) {
            converter =
                    new RowDataToAvroConverter() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public Object convert(Schema schema, Object object) {
                            return object;
                        }
                    };
        } else if (type.equals(Types.SQL_TIMESTAMP)){
            converter =
                    new RowToAvroConverters.RowDataToAvroConverter() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public Object convert(Schema schema, Object object) {
                            return ((Timestamp) object).toInstant().toEpochMilli();
                        }
                    };
        } else if (type.equals(Types.CHAR)
                || type.equals(Types.STRING)) {
            converter =
                    new RowDataToAvroConverter() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public Object convert(Schema schema, Object object) {
                            return new Utf8(object.toString());
                        }
                    };
        } else if (type.equals(Types.BIG_DEC)) {
            converter =
                    new RowDataToAvroConverter() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public Object convert(Schema schema, Object object) {
                            return ByteBuffer.wrap(((DecimalData) object).toUnscaledBytes());
                        }
                    };
        } else if (type instanceof RowTypeInfo){
            converter = createRowConverter((RowTypeInfo) type);
        } else {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }

        // wrap into nullable converter
        return new RowDataToAvroConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Schema schema, Object object) {
                if (object == null) {
                    return null;
                }

                // get actual schema if it is a nullable schema
                Schema actualSchema;
                if (schema.getType() == Schema.Type.UNION) {
                    List<Schema> types = schema.getTypes();
                    int size = types.size();
                    if (size == 2 && types.get(1).getType() == Schema.Type.NULL) {
                        actualSchema = types.get(0);
                    } else if (size == 2 && types.get(0).getType() == Schema.Type.NULL) {
                        actualSchema = types.get(1);
                    } else {
                        throw new IllegalArgumentException(
                                "The Avro schema is not a nullable type: " + schema.toString());
                    }
                } else {
                    actualSchema = schema;
                }
                return converter.convert(actualSchema, object);
            }
        };
    }

    private static RowDataToAvroConverter createRowConverter(RowTypeInfo rowTypeInfo) {
        final RowDataToAvroConverter[] fieldConverters =
                Arrays.stream(rowTypeInfo.getFieldTypes())
                        .map(RowToAvroConverters::createConverter)
                        .toArray(RowDataToAvroConverter[]::new);
        final TypeInformation<?>[] fieldTypes = rowTypeInfo.getFieldTypes();

        int length = rowTypeInfo.getArity();
        return new RowDataToAvroConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Schema schema, Object object) {
                Row row = (Row) object;
                final List<Schema.Field> fields = schema.getFields();
                final GenericRecord record = new GenericData.Record(schema);
                for (int i = 0; i < length; ++i) {
                    final Schema.Field schemaField = fields.get(i);
                    Object avroObject =
                            fieldConverters[i].convert(
                                    schemaField.schema(), row.getField(i));
                    record.put(i, avroObject);
                }
                return record;
            }
        };
    }
}

