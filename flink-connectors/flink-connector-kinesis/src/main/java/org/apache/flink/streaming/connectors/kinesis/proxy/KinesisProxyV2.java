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

package org.apache.flink.streaming.connectors.kinesis.proxy;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.kinesis.model.StartingPosition;

import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler.Visitor;

import javax.annotation.Nonnull;

import java.util.Date;

import static software.amazon.awssdk.services.kinesis.model.StartingPosition.builder;

@Internal
public class KinesisProxyV2 implements KinesisProxyV2Interface {

	private final KinesisAsyncClient kinesisAsyncClient;

	public KinesisProxyV2() {
		this.kinesisAsyncClient = KinesisAsyncClient.create();
	}

	public void subscribeToShard(
			@Nonnull final String consumerArn,
			@Nonnull final String shardId,
			@Nonnull final StartingPosition startingPosition,
			@Nonnull final Visitor visitor) {

		SubscribeToShardRequest request = SubscribeToShardRequest.builder()
			.consumerARN(consumerArn)
			.shardId(shardId)
			.startingPosition(toSdkStartingPosition(startingPosition))
			.build();

		SubscribeToShardResponseHandler responseHandler = SubscribeToShardResponseHandler
			.builder()
			.onError(t -> System.err.println("Error during stream - " + t.getMessage()))
			.subscriber(visitor)
			.build();

		kinesisAsyncClient.subscribeToShard(request, responseHandler).join();
	}

	@Nonnull
	software.amazon.awssdk.services.kinesis.model.StartingPosition toSdkStartingPosition(
			@Nonnull StartingPosition startingPosition) {

		software.amazon.awssdk.services.kinesis.model.StartingPosition.Builder builder = builder()
			.type(startingPosition.getShardIteratorType().toString());

		switch (startingPosition.getShardIteratorType()) {
			case AT_TIMESTAMP:
				builder.timestamp(((Date) startingPosition.getStartingMarker()).toInstant());
				break;
			case AT_SEQUENCE_NUMBER:
			case AFTER_SEQUENCE_NUMBER:
				builder.sequenceNumber(startingPosition.getStartingMarker().toString());
				break;
		}

		return builder.build();
	}
}
