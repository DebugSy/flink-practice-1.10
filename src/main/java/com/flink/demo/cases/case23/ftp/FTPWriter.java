/**
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

package com.flink.demo.cases.case23.ftp;

import com.flink.demo.cases.case23.flinkx.IFtpHandler;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;

/**
 * A {@link Writer} that uses {@code toString()} on the input elements and writes them to
 * the output bucket file separated by newline.
 *
 * @param <T> The type of the elements that are being written by the sink.
 */
public class FTPWriter<T> implements Writer<T> {
	private static final long serialVersionUID = 1L;

	private transient OutputStream outStream;

	private String charsetName;

	private transient Charset charset;

	private final String rowDelimiter;

	private static final String DEFAULT_ROW_DELIMITER = "\n";

	private byte[] rowDelimiterBytes;

	private long pos = 0;

	/**
	 * Creates a new {@code StringWriter} that uses {@code "UTF-8"} charset to convert
	 * strings to bytes.
	 */
	public FTPWriter() {
		this("UTF-8", DEFAULT_ROW_DELIMITER);
	}

	/**
	 * Creates a new {@code StringWriter} that uses the given charset to convert
	 * strings to bytes.
	 *
	 * @param charsetName Name of the charset to be used, must be valid input for {@code Charset.forName(charsetName)}
	 */
	public FTPWriter(String charsetName) {
		this(charsetName, DEFAULT_ROW_DELIMITER);
	}

	/**
	 * Creates a new {@code StringWriter} that uses the given charset and row delimiter to convert
	 * strings to bytes.
	 *
	 * @param charsetName Name of the charset to be used, must be valid input for {@code Charset.forName(charsetName)}
	 * @param rowDelimiter Parameter that specifies which character to use for delimiting rows
	 */
	public FTPWriter(String charsetName, String rowDelimiter) {
		this.charsetName = charsetName;
		this.rowDelimiter = rowDelimiter;
	}

	protected FTPWriter(FTPWriter<T> other) {
		this.charsetName = other.charsetName;
		this.rowDelimiter = other.rowDelimiter;
	}

	/**
	 * Returns the current output stream, if the stream is open.
	 */
	protected OutputStream getStream() {
		if (outStream == null) {
			throw new IllegalStateException("Output stream has not been opened");
		}
		return outStream;
	}

	@Override
	public void open(IFtpHandler ftpHandler, Path path) throws IOException {
		try {
			if (outStream != null) {
				throw new IllegalStateException("Writer has already been opened");
			}
			outStream = ftpHandler.getOutputStream(path.toString());
			this.charset = Charset.forName(charsetName);
			this.rowDelimiterBytes = rowDelimiter.getBytes(charset);
		}
		catch (IllegalCharsetNameException e) {
			throw new IOException("The charset " + charsetName + " is not valid.", e);
		}
		catch (UnsupportedCharsetException e) {
			throw new IOException("The charset " + charsetName + " is not supported.", e);
		}
	}

	@Override
	public void write(T element) throws IOException {
		OutputStream outputStream = getStream();
		byte[] bytes = element.toString().getBytes(charset);
		pos += bytes.length;
		outputStream.write(bytes);
		outputStream.write(rowDelimiterBytes);
	}

	@Override
	public void flush() throws IOException {
		if (outStream == null) {
			throw new IllegalStateException("Writer is not open");
		}
		outStream.flush();
	}

	@Override
	public long getPos() throws IOException {
		return pos;
	}

	@Override
	public void close(IFtpHandler ftpHandler) throws IOException {
		if (outStream != null) {
			flush();
			outStream.close();
			outStream = null;
			ftpHandler.completePendingCommand();
		}
	}

	@Override
	public FTPWriter<T> duplicate() {
		return new FTPWriter<>(this);
	}

	String getCharsetName() {
		return charsetName;
	}

	public String getRowDelimiter() {
		return rowDelimiter;
	}
}
