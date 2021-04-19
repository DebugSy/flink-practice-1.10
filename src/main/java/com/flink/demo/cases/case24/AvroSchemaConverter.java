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

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import java.util.List;


public class AvroSchemaConverter {

  public static Schema convertToSchema(RowTypeInfo rowTypeInfo) {
    return convertToSchema(rowTypeInfo, "record");
  }

  public static Schema convertToSchema(RowTypeInfo rowTypeInfo, String rowName) {
    String[] fieldNames = rowTypeInfo.getFieldNames();
    // we have to make sure the record name is different in a Schema
    SchemaBuilder.FieldAssembler<Schema> builder =
            SchemaBuilder.builder().record(rowName).fields();
    for (int i = 0; i < rowTypeInfo.getArity(); i++) {
      String fieldName = fieldNames[i];
      TypeInformation<Object> type = rowTypeInfo.getTypeAt(i);
      SchemaBuilder.GenericDefault<Schema> fieldBuilder =
              builder.name(fieldName)
                      .type(convertToSchema(type, rowName + "_" + fieldName));

      if (false) {
        builder = fieldBuilder.withDefault(null);
      } else {
        builder = fieldBuilder.noDefault();
      }
    }
    Schema record = builder.endRecord();
    return false ? nullableSchema(record) : record;
  }

  public static Schema convertToSchema(TypeInformation<?> type) {
    return convertToSchema(type, "record");
  }

  public static Schema convertToSchema(TypeInformation<?> type, String rowName) {
    boolean nullable = false;
    if (type.equals(Types.VOID)) {
      return SchemaBuilder.builder().nullType();
    } else if (type.equals(Types.BOOLEAN)) {
      Schema bool = SchemaBuilder.builder().booleanType();
      return nullable ? nullableSchema(bool) : bool;
    } else if (type.equals(Types.BYTE) || type.equals(Types.SHORT) || type.equals(Types.INT)) {
      Schema integer = SchemaBuilder.builder().intType();
      return nullable ? nullableSchema(integer) : integer;
    } else if (type.equals(Types.BIG_INT)) {
      Schema bigint = SchemaBuilder.builder().longType();
      return nullable ? nullableSchema(bigint) : bigint;
    } else if (type.equals(Types.FLOAT)) {
      Schema f = SchemaBuilder.builder().floatType();
      return nullable ? nullableSchema(f) : f;
    } else if (type.equals(Types.DOUBLE)) {
      Schema d = SchemaBuilder.builder().doubleType();
      return nullable ? nullableSchema(d) : d;
    } else if (type.equals(Types.CHAR)) {
      Schema binary = SchemaBuilder.builder().bytesType();
      return nullable ? nullableSchema(binary) : binary;
    } else if (type.equals(Types.STRING)) {
      Schema string = SchemaBuilder.builder().stringType();
      return nullable ? nullableSchema(string) : string;
    } else if (type.equals(Types.SQL_DATE)) {
      // use int to represents Date
      Schema date = LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType());
      return nullable ? nullableSchema(date) : date;
    } else if (type.equals(Types.BIG_DEC)) {
      throw new UnsupportedOperationException(
              "Unsupported to derive Schema for type: " + type);
    } else if (type.equals(Types.SQL_TIMESTAMP)) {
      Schema timestamp = LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder().longType());
      return nullable ? nullableSchema(timestamp) : timestamp;
    } else {
      throw new UnsupportedOperationException(
              "Unsupported to derive Schema for type: " + type);
    }
  }

  /** Returns schema with nullable true. */
  private static Schema nullableSchema(Schema schema) {
    return schema.isNullable()
        ? schema
        : Schema.createUnion(SchemaBuilder.builder().nullType(), schema);
  }
}

