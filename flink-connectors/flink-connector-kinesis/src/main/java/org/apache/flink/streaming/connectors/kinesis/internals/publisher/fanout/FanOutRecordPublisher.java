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

package org.apache.flink.streaming.connectors.kinesis.internals.publisher.fanout;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordBatch;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.StartingPosition;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyV2Interface;

import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import javax.annotation.Nonnull;

import java.util.Date;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class FanOutRecordPublisher implements RecordPublisher {

	private Consumer<RecordBatch> consumer;

	private final String consumerArn;

	private final String shardId;

	private final KinesisProxyV2Interface kinesisProxy;

	private final StreamShardHandle subscribedShard;

	public FanOutRecordPublisher(
			@Nonnull final String consumerArn,
			@Nonnull final String shardId,
			@Nonnull final MetricGroup metricGroup,
			@Nonnull final StreamShardHandle subscribedShard,
			@Nonnull final KinesisProxyV2Interface kinesisProxy) {
		this.consumerArn = consumerArn;
		this.shardId = shardId;
		this.subscribedShard = subscribedShard;
		this.kinesisProxy = kinesisProxy;
	}

	@Override
	public void subscribe(
			@Nonnull final StartingPosition startingPosition,
			@Nonnull final Consumer<RecordBatch> consumer) throws InterruptedException {
		this.consumer = consumer;
	}

	@Override
	public void run(@Nonnull SequenceNumber lastSequenceNumber) throws InterruptedException {
		final StartingPosition startingPosition = StartingPosition.fromSequenceNumber(lastSequenceNumber);
		kinesisProxy.subscribeToShard(consumerArn, shardId, startingPosition, new SubscribeToShardResponseHandler.Visitor() {

			@Override
			public void visit(SubscribeToShardEvent event) {
				final RecordBatch recordBatch = new RecordBatch(event
					.records()
					.stream()
					.map(FanOutRecordPublisher.this::toSdkV1Record)
					.collect(Collectors.toList()), subscribedShard, event.millisBehindLatest());

				consumer.accept(recordBatch);
			}
		});
	}

	@Override
	public boolean isComplete() {
		return false;
	}

	com.amazonaws.services.kinesis.model.Record toSdkV1Record(@Nonnull final Record record) {
		return new com.amazonaws.services.kinesis.model.Record()
			.withData(record.data().asByteBuffer())
			.withSequenceNumber(record.sequenceNumber())
			.withPartitionKey(record.partitionKey())
			.withEncryptionType(record.encryptionType().name())
			.withApproximateArrivalTimestamp(new Date(record.approximateArrivalTimestamp().toEpochMilli()));
	}
}
