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

package com.flink.demo.cases.case27.datastream.parquet;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.formats.parquet.ParquetBuilder;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.utils.SerializableConfiguration;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.util.HashMap;

import static org.apache.parquet.hadoop.ParquetOutputFormat.*;
import static org.apache.parquet.hadoop.codec.CodecConfig.getParquetCompressionCodec;

/**
 * {@link Row} of {@link ParquetWriter.Builder}.
 */
public class ParquetRowBuilder extends ParquetWriter.Builder<Row, ParquetRowBuilder> {

	private final String[] fieldNames;
	private final String[] fieldTypes;
	private final boolean utcTimestamp;

	public ParquetRowBuilder(
			OutputFile path,
			String[] fieldNames,
			String[] fieldTypes,
			boolean utcTimestamp) {
		super(path);
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
		this.utcTimestamp = utcTimestamp;
	}

	@Override
	protected ParquetRowBuilder self() {
		return this;
	}

	@Override
	protected WriteSupport<Row> getWriteSupport(Configuration conf) {
		return new ParquetWriteSupport();
	}

	private class ParquetWriteSupport extends WriteSupport<Row> {

		private MessageType schema = convertToParquetMessageType(fieldNames, fieldTypes);
		private ParquetRowWriter writer;

		@Override
		public WriteContext init(Configuration configuration) {
			return new WriteContext(schema, new HashMap<>());
		}

		@Override
		public void prepareForWrite(RecordConsumer recordConsumer) {
			this.writer = new ParquetRowWriter(
					recordConsumer,
					fieldNames,
					fieldTypes,
					schema,
					utcTimestamp);
		}

		@Override
		public void write(Row record) {
			try {
				this.writer.write(record);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		private MessageType convertToParquetMessageType(String[] fieldNames, String[] fieldTypes) {
			Types.MessageTypeBuilder builder = Types.buildMessage();
			for (int i = 0; i < fieldNames.length; i++) {
				String name = fieldNames[i];
				String type = fieldTypes[i];
				String dateType = type.startsWith("decimal") ? "decimal" : type;
				switch (dateType) {
					case "string":
						builder.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(name);
						break;
					case "byte":
						builder.optional(PrimitiveType.PrimitiveTypeName.INT32).as(OriginalType.INT_8).named(name);
						break;
					case "short":
					case "int":
						builder.optional(PrimitiveType.PrimitiveTypeName.INT32).named(name);
						break;
					case "bigint":
						builder.optional(PrimitiveType.PrimitiveTypeName.INT64).named(name);
						break;
					case "float":
						builder.optional(PrimitiveType.PrimitiveTypeName.FLOAT).named(name);
						break;
					case "double":
						builder.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named(name);
						break;
					case "boolean":
						builder.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN).named(name);
						break;
					case "date":
						builder.optional(PrimitiveType.PrimitiveTypeName.INT32).as(OriginalType.DATE).named(name);
						break;
					case "timestamp":
						builder.optional(PrimitiveType.PrimitiveTypeName.INT96).named(name);
						break;
					case "binary":
						builder.optional(PrimitiveType.PrimitiveTypeName.BINARY).named(name);
						break;
					case "decimal":
						DecimalContainer decimalContainer = ParquetHelper.convertTypeDecimal(type);
						builder.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
								.length(ParquetHelper.computeMinBytesForPrecision(decimalContainer.getPrecision()))
								.as(OriginalType.DECIMAL)
								.precision(decimalContainer.getPrecision())
								.scale(decimalContainer.getScale())
								.named(name);
						break;
					default:
						throw new RuntimeException("unknown type: " + type);
				}
			}
			return builder.named("flink_schema");
		}
	}

	/**
	 * Create a parquet {@link BulkWriter.Factory}.
	 *
	 * @param rowType row type of parquet table.
	 * @param conf hadoop configuration.
	 * @param utcTimestamp Use UTC timezone or local timezone to the conversion between epoch time
	 *                     and LocalDateTime. Hive 0.x/1.x/2.x use local timezone. But Hive 3.x
	 *                     use UTC timezone.
	 */
	public static ParquetWriterFactory<Row> createWriterFactory(
			String[] fieldNames,
			String[] filedTypes,
			Configuration conf,
			boolean utcTimestamp) {
		return new ParquetWriterFactory<>(
				new FlinkParquetBuilder(fieldNames, filedTypes, conf, utcTimestamp));
	}

	/**
	 * Flink Row {@link ParquetBuilder}.
	 */
	public static class FlinkParquetBuilder implements ParquetBuilder<Row> {

		private final String[] fieldNames;
		private final String[] filedTypes;
		private final SerializableConfiguration configuration;
		private final boolean utcTimestamp;

		public FlinkParquetBuilder(
				String[] fieldNames,
				String[] filedTypes,
				Configuration conf,
				boolean utcTimestamp) {
			this.fieldNames = fieldNames;
			this.filedTypes = filedTypes;
			this.configuration = new SerializableConfiguration(conf);
			this.utcTimestamp = utcTimestamp;
		}

		@Override
		public ParquetWriter<Row> createWriter(OutputFile out) throws IOException {
			Configuration conf = configuration.conf();
			return new ParquetRowBuilder(out, fieldNames, filedTypes, utcTimestamp)
					.withCompressionCodec(getParquetCompressionCodec(conf))
					.withRowGroupSize(getBlockSize(conf))
					.withPageSize(getPageSize(conf))
					.withDictionaryPageSize(getDictionaryPageSize(conf))
					.withMaxPaddingSize(conf.getInt(
							MAX_PADDING_BYTES, ParquetWriter.MAX_PADDING_SIZE_DEFAULT))
					.withDictionaryEncoding(getEnableDictionary(conf))
					.withValidation(getValidation(conf))
					.withWriterVersion(getWriterVersion(conf))
					.withConf(conf).build();
		}
	}
}
