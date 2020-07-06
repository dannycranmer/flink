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

package org.apache.flink.streaming.connectors.kinesis.internals.publisher.polling;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisherFactory;
import org.apache.flink.streaming.connectors.kinesis.metrics.PollingRecordPublisherMetricsReporter;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyInterface;

import javax.annotation.Nonnull;

import java.util.Properties;

/**
 * A {@link RecordPublisher} factory used to create instances of {@link PollingRecordPublisher}.
 */
@Internal
public class PollingRecordPublisherFactory implements RecordPublisherFactory<KinesisProxyInterface> {

	/**
	 * Create a {@link PollingRecordPublisher}.
	 * @param consumerConfig the consumer configuration properties
	 * @param metricGroup the metric group to report metrics to
	 * @param streamShardHandle the shard this consumer is subscribed to
	 * @param kinesisProxy the proxy instance to interact with Kinesis
	 * @return a {@link PollingRecordPublisher}
	 */
	@Nonnull
	@Override
	public RecordPublisher create(@Nonnull final Properties consumerConfig,
			@Nonnull final MetricGroup metricGroup,
			@Nonnull final StreamShardHandle streamShardHandle,
			@Nonnull final KinesisProxyInterface kinesisProxy) {
		final int maxNumberOfRecordsPerFetch = Integer.parseInt(consumerConfig.getProperty(
			ConsumerConfigConstants.SHARD_GETRECORDS_MAX,
			Integer.toString(ConsumerConfigConstants.DEFAULT_SHARD_GETRECORDS_MAX)));
		final long fetchIntervalMillis = Long.parseLong(consumerConfig.getProperty(
			ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS,
			Long.toString(ConsumerConfigConstants.DEFAULT_SHARD_GETRECORDS_INTERVAL_MILLIS)));
		final boolean useAdaptiveReads = Boolean.parseBoolean(consumerConfig.getProperty(
			ConsumerConfigConstants.SHARD_USE_ADAPTIVE_READS,
			Boolean.toString(ConsumerConfigConstants.DEFAULT_SHARD_USE_ADAPTIVE_READS)));

		final PollingRecordPublisherMetricsReporter metricsReporter = new PollingRecordPublisherMetricsReporter(metricGroup);

		final PollingRecordPublisher pollingRecordPublisher = PollingRecordPublisher
				.builder()
				.withKinesisProxy(kinesisProxy)
				.withSubscribedShard(streamShardHandle)
				.withMetricsReporter(metricsReporter)
				.withFetchIntervalMillis(fetchIntervalMillis)
				.withMaxNumberOfRecordsPerFetch(maxNumberOfRecordsPerFetch)
				.build();

		if (useAdaptiveReads) {
			return AdaptivePollingRecordPublisher
				.builder()
				.withPollingRecordPublisher(pollingRecordPublisher)
				.withFetchIntervalMillis(fetchIntervalMillis)
				.withMetricsReporter(metricsReporter)
				.withMaxNumberOfRecordsPerFetch(maxNumberOfRecordsPerFetch)
				.build();
		}

		return pollingRecordPublisher;
	}
}
