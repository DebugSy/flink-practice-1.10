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

import com.flink.demo.cases.case27.datastream.parquet.DecimalContainer;
import com.flink.demo.cases.case27.datastream.parquet.ParquetHelper;
import org.apache.flink.formats.parquet.row.ParquetRowDataWriter;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.util.Arrays;

import static com.flink.demo.cases.case27.datastream.parquet.DecimalContainer.*;
import static org.apache.flink.formats.parquet.utils.ParquetSchemaConverter.computeMinBytesForDecimalPrecision;
import static org.apache.flink.formats.parquet.vector.reader.TimestampColumnReader.*;

/**
 * Writes a record to the Parquet API with the expected schema in order to be written to a file.
 */
public class ParquetRowWriter {

	private final RecordConsumer recordConsumer;
	private final boolean utcTimestamp;

	private final FieldWriter[] filedWriters;
	private final String[] fieldNames;
	private final String[] fieldTypes;

	public ParquetRowWriter(
			RecordConsumer recordConsumer,
			String[] fieldNames,
			String[] fieldTypes,
			GroupType schema,
			boolean utcTimestamp) {
		this.recordConsumer = recordConsumer;
		this.utcTimestamp = utcTimestamp;

		this.filedWriters = new FieldWriter[fieldNames.length];
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
		for (int i = 0; i < fieldNames.length; i++) {
			this.filedWriters[i] = createWriter(fieldTypes[i], schema.getType(i));
		}
	}

	/**
	 * It writes a record to Parquet.
	 *
	 * @param record Contains the record that is going to be written.
	 */
	public void write(final Row record) {
		recordConsumer.startMessage();
		for (int i = 0; i < filedWriters.length; i++) {
			Object field = record.getField(i);
			if (field != null) {
				String fieldName = fieldNames[i];
				FieldWriter writer = filedWriters[i];

				recordConsumer.startField(fieldName, i);
				writer.write(record, i);
				recordConsumer.endField(fieldName, i);
			}
		}
		recordConsumer.endMessage();
	}

	private FieldWriter createWriter(String t, Type type) {
		if (type.isPrimitive()) {
			switch (t) {
				case "string":
					return new StringWriter();
				case "boolean":
					return new BooleanWriter();
				case "decimal":
					DecimalContainer decimalContainer = ParquetHelper.convertTypeDecimal(t);
					return createDecimalWriter(decimalContainer.getPrecision(), decimalContainer.getScale());
				case "byte":
					return new ByteWriter();
				case "short":
					return new ShortWriter();
				case "date":
				case "int":
					return new IntWriter();
				case "bigint":
					return new LongWriter();
				case "float":
					return new FloatWriter();
				case "double":
					return new DoubleWriter();
				case "timestamp":
					return new TimestampWriter();
				default:
					throw new UnsupportedOperationException("Unsupported type: " + type);
			}
		} else {
			throw new IllegalArgumentException("Unsupported  data type: " + t);
		}
	}

	private interface FieldWriter {

		void write(Row row, int ordinal);
	}

	private class BooleanWriter implements FieldWriter {

		@Override
		public void write(Row row, int ordinal) {
			recordConsumer.addBoolean((Boolean) row.getField(ordinal));
		}
	}

	private class ByteWriter implements FieldWriter {

		@Override
		public void write(Row row, int ordinal) {
			recordConsumer.addInteger((Byte) row.getField(ordinal));
		}
	}

	private class ShortWriter implements FieldWriter {

		@Override
		public void write(Row row, int ordinal) {
			recordConsumer.addInteger((Short) row.getField(ordinal));
		}
	}

	private class LongWriter implements FieldWriter {

		@Override
		public void write(Row row, int ordinal) {
			recordConsumer.addLong((Long) row.getField(ordinal));
		}
	}

	private class FloatWriter implements FieldWriter {

		@Override
		public void write(Row row, int ordinal) {
			recordConsumer.addFloat((Float) row.getField(ordinal));
		}
	}

	private class DoubleWriter implements FieldWriter {

		@Override
		public void write(Row row, int ordinal) {
			recordConsumer.addDouble((Double) row.getField(ordinal));
		}
	}

	private class StringWriter implements FieldWriter {

		@Override
		public void write(Row row, int ordinal) {
			recordConsumer.addBinary(
				Binary.fromReusedByteArray(row.getField(ordinal).toString().getBytes()));
		}
	}

	private class BinaryWriter implements FieldWriter {

		@Override
		public void write(Row row, int ordinal) {
			recordConsumer.addBinary(
					Binary.fromReusedByteArray((byte[]) row.getField(ordinal)));
		}
	}

	private class IntWriter implements FieldWriter {

		@Override
		public void write(Row row, int ordinal) {
			recordConsumer.addInteger((Integer) row.getField(ordinal));
		}
	}

	/**
	 * We only support INT96 bytes now, julianDay(4) + nanosOfDay(8).
	 * See https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp
	 * TIMESTAMP_MILLIS and TIMESTAMP_MICROS are the deprecated ConvertedType.
	 */
	private class TimestampWriter implements FieldWriter {

		@Override
		public void write(Row row, int ordinal) {
			recordConsumer.addBinary(timestampToInt96((Timestamp) row.getField(ordinal)));
		}
	}

	private Binary timestampToInt96(Timestamp timestamp) {
		int julianDay;
		long nanosOfDay;
		if (utcTimestamp) {
			long mills = timestamp.getTime();
			julianDay = (int) ((mills / MILLIS_IN_DAY) + JULIAN_EPOCH_OFFSET_DAYS);
			nanosOfDay = (mills % MILLIS_IN_DAY) * NANOS_PER_MILLISECOND + timestamp.getNanos();
		} else {
			long mills = timestamp.getTime();
			julianDay = (int) ((mills / MILLIS_IN_DAY) + JULIAN_EPOCH_OFFSET_DAYS);
			nanosOfDay = ((mills % MILLIS_IN_DAY) / 1000) * NANOS_PER_SECOND + timestamp.getNanos();
		}

		ByteBuffer buf = ByteBuffer.allocate(12);
		buf.order(ByteOrder.LITTLE_ENDIAN);
		buf.putLong(nanosOfDay);
		buf.putInt(julianDay);
		buf.flip();
		return Binary.fromConstantByteBuffer(buf);
	}

	private FieldWriter createDecimalWriter(int precision, int scale) {
		Preconditions.checkArgument(
			precision <= MAX_PRECISION,
			"Decimal precision %s exceeds max precision %s",
			precision, MAX_PRECISION);

		/*
		 * This is optimizer for UnscaledBytesWriter.
		 */
		class LongUnscaledBytesWriter implements FieldWriter {
			private final int numBytes;
			private final int initShift;
			private final byte[] decimalBuffer;

			private LongUnscaledBytesWriter() {
				this.numBytes = computeMinBytesForDecimalPrecision(precision);
				this.initShift = 8 * (numBytes - 1);
				this.decimalBuffer = new byte[numBytes];
			}

			@Override
			public void write(Row row, int ordinal) {
				BigDecimal decimal = (BigDecimal) row.getField(ordinal);
				long unscaledLong = decimal.unscaledValue().longValueExact();
				int i = 0;
				int shift = initShift;
				while (i < numBytes) {
					decimalBuffer[i] = (byte) (unscaledLong >> shift);
					i += 1;
					shift -= 8;
				}

				recordConsumer.addBinary(Binary.fromReusedByteArray(decimalBuffer, 0, numBytes));
			}
		}

		class UnscaledBytesWriter implements FieldWriter {
			private final int numBytes;
			private final byte[] decimalBuffer;

			private UnscaledBytesWriter() {
				this.numBytes = computeMinBytesForDecimalPrecision(precision);
				this.decimalBuffer = new byte[numBytes];
			}

			@Override
			public void write(Row row, int ordinal) {
				BigDecimal decimal = (BigDecimal) row.getField(ordinal);
				byte[] bytes = decimal.unscaledValue().toByteArray();
				byte[] writtenBytes;
				if (bytes.length == numBytes) {
					// Avoid copy.
					writtenBytes = bytes;
				} else {
					byte signByte = bytes[0] < 0 ? (byte) -1 : (byte) 0;
					Arrays.fill(decimalBuffer, 0, numBytes - bytes.length, signByte);
					System.arraycopy(bytes, 0, decimalBuffer, numBytes - bytes.length, bytes.length);
					writtenBytes = decimalBuffer;
				}
				recordConsumer.addBinary(Binary.fromReusedByteArray(writtenBytes, 0, numBytes));
			}
		}

		// 1 <= precision <= 18, writes as FIXED_LEN_BYTE_ARRAY
		// optimizer for UnscaledBytesWriter
		if (precision <= MAX_INT_DIGITS || (precision <= MAX_LONG_DIGITS && precision > MAX_INT_DIGITS)) {
			return new LongUnscaledBytesWriter();
		}

		// 19 <= precision <= 38, writes as FIXED_LEN_BYTE_ARRAY
		return new UnscaledBytesWriter();
	}
}
