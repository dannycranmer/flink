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

package org.apache.flink.streaming.connectors.kinesis.model;

import com.amazonaws.services.kinesis.model.ShardIteratorType;
import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests for {@link StartingPosition}.
 */
public class StartingPositionTest {

	@Test
	public void testStartingPositionFromTimestamp() {
		Date date = new Date();
		StartingPosition position = StartingPosition.fromTimestamp(date);
		assertEquals(ShardIteratorType.AT_TIMESTAMP, position.getShardIteratorType());
		assertEquals(date, position.getStartingMarker());
	}

	@Test
	public void testStartingPositionFromSequenceNumber() {
		SequenceNumber sequenceNumber = new SequenceNumber("100");
		StartingPosition position = StartingPosition.fromSequenceNumber(sequenceNumber);
		assertEquals(ShardIteratorType.AFTER_SEQUENCE_NUMBER, position.getShardIteratorType());
		assertEquals("100", position.getStartingMarker());
	}

	@Test
	public void testStartingPositionFromAggregatedSequenceNumber() {
		SequenceNumber sequenceNumber = new SequenceNumber("200", 3);
		StartingPosition position = StartingPosition.fromSequenceNumber(sequenceNumber);
		assertEquals(ShardIteratorType.AT_SEQUENCE_NUMBER, position.getShardIteratorType());
		assertEquals("200", position.getStartingMarker());
	}

	@Test
	public void testStartingPositionFromSentinelEarliest() {
		SequenceNumber sequenceNumber = SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM.get();
		StartingPosition position = StartingPosition.fromSequenceNumber(sequenceNumber);
		assertEquals(ShardIteratorType.TRIM_HORIZON, position.getShardIteratorType());
		assertNull(position.getStartingMarker());
	}

	@Test
	public void testStartingPositionFromSentinelLatest() {
		SequenceNumber sequenceNumber = SentinelSequenceNumber.SENTINEL_LATEST_SEQUENCE_NUM.get();
		StartingPosition position = StartingPosition.fromSequenceNumber(sequenceNumber);
		assertEquals(ShardIteratorType.LATEST, position.getShardIteratorType());
		assertNull(position.getStartingMarker());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testStartingPositionFromSentinelEnding() {
		SequenceNumber sequenceNumber = SentinelSequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM.get();
		StartingPosition position = StartingPosition.fromSequenceNumber(sequenceNumber);
		assertEquals(ShardIteratorType.LATEST, position.getShardIteratorType());
		assertNull(position.getStartingMarker());
	}
}
