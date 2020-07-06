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

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.StartingPosition;

import javax.annotation.Nonnull;

import java.util.function.Consumer;

/**
 * A {@code RecordPublisher} will consume records from an external stream and deliver them to the registered subscriber.
 */
@Internal
public interface RecordPublisher {

	/**
	 * Register the record consumer that will receive records published from Kinesis at the given starting position.
	 *
	 * @param startingPosition the position in the stream to start consuming records from
	 * @param recordConsumer the record consumer that received records should be published to
	 * @throws InterruptedException
	 */
	void subscribe(@Nonnull StartingPosition startingPosition, @Nonnull Consumer<RecordBatch> recordConsumer) throws InterruptedException;

	/**
	 * Run the record publisher. Records will be consumed from the stream and published to the consumer.
	 *
	 * @param lastSequenceNumber
	 * @throws InterruptedException
	 */
	void run(@Nonnull SequenceNumber lastSequenceNumber) throws InterruptedException;

	/**
	 * Returns {@code true} if the stream shard has been fully consumed.
	 *
	 * @return {@code true} if the stream shard has been fully consumed
	 */
	boolean isComplete();
}
