/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kinesis.internals.publisher;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestUtils;

import com.amazonaws.services.kinesis.model.HashKeyRange;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link RecordBatch}.
 */
public class RecordBatchTest {

	@Test
	public void testDeaggregateRecordsPassThrough() {
		RecordBatch result = new RecordBatch(Arrays.asList(
			record("1"),
			record("2"),
			record("3"),
			record("4")
		), handle(), 100L);

		assertEquals(4, result.getAggregatedRecordSize());
		assertEquals(4, result.getDeaggregatedRecordSize());
		assertEquals(128, result.getTotalSizeInBytes());
	}

	@Test
	public void testDeaggregateRecordsWithAggregatedRecords() {
		final List<Record> records = TestUtils.createAggregatedRecordBatch(5, 5, new AtomicInteger());
		RecordBatch result = new RecordBatch(records, handle(), 100L);

		assertEquals(5, result.getAggregatedRecordSize());
		assertEquals(25, result.getDeaggregatedRecordSize());
		assertEquals(25 * 1024, result.getTotalSizeInBytes());
	}

	@Test
	public void testGetMillisBehindLatest() {
		RecordBatch result = new RecordBatch(singletonList(record("1")), handle(), 100L);

		assertEquals(Long.valueOf(100), result.getMillisBehindLatest());
	}

	@Nonnull
	private Record record(@Nonnull final String sequenceNumber) {
		byte[] data = RandomStringUtils.randomAlphabetic(32)
			.getBytes(ConfigConstants.DEFAULT_CHARSET);

		return new Record()
			.withData(ByteBuffer.wrap(data))
			.withPartitionKey("pk")
			.withSequenceNumber(sequenceNumber);
	}

	@Nonnull
	private StreamShardHandle handle() {
		final Shard shard = new Shard()
			.withHashKeyRange(new HashKeyRange()
				.withStartingHashKey("0")
				.withEndingHashKey(new BigInteger(StringUtils.repeat("FF", 16), 16).toString()))
			.withShardId("000000");

		return new StreamShardHandle("stream-name", shard);
	}

}
