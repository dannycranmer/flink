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

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisherFactory;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyV2Interface;

import javax.annotation.Nonnull;

import java.util.Properties;

/**
 * A {@link RecordPublisher} factory used to create instances of {@link FanOutRecordPublisher}.
 */
@Internal
public class FanOutRecordPublisherFactory implements RecordPublisherFactory<KinesisProxyV2Interface> {

	/**
	 * Create a {@link FanOutRecordPublisherFactory}.
	 * @param consumerConfig the consumer configuration properties
	 * @param metricGroup the metric group to report metrics to
	 * @param streamShardHandle the shard this consumer is subscribed to
	 * @param kinesisProxy the proxy instance to interact with Kinesis
	 * @return a {@link FanOutRecordPublisherFactory}
	 */
	@Nonnull
	@Override
	public RecordPublisher create(@Nonnull final Properties consumerConfig,
			@Nonnull final MetricGroup metricGroup,
			@Nonnull final StreamShardHandle streamShardHandle,
			@Nonnull final KinesisProxyV2Interface kinesisProxy) {

		return new FanOutRecordPublisher("", "", metricGroup, streamShardHandle, kinesisProxy);
	}
}
