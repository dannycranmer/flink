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
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordBatch;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher;
import org.apache.flink.streaming.connectors.kinesis.metrics.PollingRecordPublisherMetricsReporter;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.StartingPosition;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyInterface;

import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.function.Consumer;

import static com.amazonaws.services.kinesis.model.ShardIteratorType.LATEST;

/**
 * A {@link RecordPublisher} that will read records from Kinesis and forward them to the subscriber.
 * Records are consumed by polling the GetRecords KDS API using a ShardIterator.
 */
@Internal
public class PollingRecordPublisher implements RecordPublisher {

	private static final Logger LOG = LoggerFactory.getLogger(PollingRecordPublisher.class);

	private Consumer<RecordBatch> consumer;

	private final PollingRecordPublisherMetricsReporter metricsReporter;

	private final KinesisProxyInterface kinesisProxy;

	private final StreamShardHandle subscribedShard;

	private String nextShardItr;

	private final int maxNumberOfRecordsPerFetch;

	private final long fetchIntervalMillis;

	private PollingRecordPublisher(@Nonnull final Builder builder) {
		this.subscribedShard = builder.subscribedShard;
		this.metricsReporter = builder.metricsReporter;
		this.kinesisProxy = builder.kinesisProxy;
		this.maxNumberOfRecordsPerFetch = builder.maxNumberOfRecordsPerFetch;
		this.fetchIntervalMillis = builder.fetchIntervalMillis;
	}

	@Override
	public void subscribe(
			@Nonnull final StartingPosition startingPosition,
			@Nonnull final Consumer<RecordBatch> consumer) throws InterruptedException {
		this.consumer = consumer;
		this.nextShardItr = getShardIterator(startingPosition);
	}

	@Override
	public void run(@Nonnull final SequenceNumber lastSequenceNumber) throws InterruptedException {
		run(lastSequenceNumber, maxNumberOfRecordsPerFetch);
	}

	public void run(@Nonnull final SequenceNumber lastSequenceNumber, int maxNumberOfRecords) throws InterruptedException {
		metricsReporter.setMaxNumberOfRecordsPerFetch(maxNumberOfRecords);

		GetRecordsResult result = getRecords(nextShardItr, lastSequenceNumber, maxNumberOfRecords);

		consumer.accept(new RecordBatch(result.getRecords(), subscribedShard, result.getMillisBehindLatest()));

		nextShardItr = result.getNextShardIterator();
	}

	@Override
	public boolean isComplete() {
		return nextShardItr == null;
	}

	/**
	 * Calls {@link KinesisProxyInterface#getRecords(String, int)}, while also handling unexpected
	 * AWS {@link ExpiredIteratorException}s to assure that we get results and don't just fail on
	 * such occasions. The returned shard iterator within the successful {@link GetRecordsResult} should
	 * be used for the next call to this method.
	 *
	 * <p>Note: it is important that this method is not called again before all the records from the last result have been
	 * fully collected with {@code ShardConsumer#deserializeRecordForCollectionAndUpdateState(UserRecord)}, otherwise
	 * {@code ShardConsumer#lastSequenceNum} may refer to a sub-record in the middle of an aggregated record, leading to
	 * incorrect shard iteration if the iterator had to be refreshed.
	 *
	 * @param shardItr shard iterator to use
	 * @param maxNumberOfRecords the maximum number of records to fetch for this getRecords attempt
	 * @return get records result
	 */
	@Nonnull
	private GetRecordsResult getRecords(@Nonnull String shardItr,
			@Nonnull final SequenceNumber lastSequenceNumber,
			int maxNumberOfRecords) throws InterruptedException {
		GetRecordsResult getRecordsResult = null;
		while (getRecordsResult == null) {
			try {
				getRecordsResult = kinesisProxy.getRecords(shardItr, maxNumberOfRecords);
			} catch (ExpiredIteratorException | InterruptedException eiEx) {
				LOG.warn("Encountered an unexpected expired iterator {} for shard {};" +
					" refreshing the iterator ...", shardItr, subscribedShard);

				shardItr = getShardIterator(StartingPosition.fromSequenceNumber(lastSequenceNumber));

				// sleep for the fetch interval before the next getRecords attempt with the refreshed iterator
				if (fetchIntervalMillis != 0) {
					Thread.sleep(fetchIntervalMillis);
				}
			}
		}
		return getRecordsResult;
	}

	/**
	 * Returns a shard iterator for the given {@link SequenceNumber}.
	 *
	 * @return shard iterator
	 */
	@Nullable
	protected String getShardIterator(@Nonnull final StartingPosition startingPosition) throws InterruptedException {
		if (startingPosition.getShardIteratorType() == LATEST && subscribedShard.isClosed()) {
			return null;
		}

		return kinesisProxy.getShardIterator(
			subscribedShard,
			startingPosition.getShardIteratorType().toString(),
			startingPosition.getStartingMarker());
	}

	@Nonnull
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * A builder used to build immutable instances of {@link PollingRecordPublisher} using a builder pattern.
	 */
	public static class Builder {

		private StreamShardHandle subscribedShard;

		private PollingRecordPublisherMetricsReporter metricsReporter;

		private KinesisProxyInterface kinesisProxy;

		private int maxNumberOfRecordsPerFetch = ConsumerConfigConstants.DEFAULT_SHARD_GETRECORDS_MAX;

		private long fetchIntervalMillis = ConsumerConfigConstants.DEFAULT_SHARD_GETRECORDS_INTERVAL_MILLIS;

		@Nonnull
		public Builder withSubscribedShard(@Nonnull final StreamShardHandle subscribedShard) {
			this.subscribedShard = subscribedShard;
			return this;
		}

		@Nonnull
		public Builder withKinesisProxy(@Nonnull final KinesisProxyInterface kinesisProxy) {
			this.kinesisProxy = kinesisProxy;
			return this;
		}

		@Nonnull
		public Builder withMetricsReporter(@Nonnull final PollingRecordPublisherMetricsReporter metricsReporter) {
			this.metricsReporter = metricsReporter;
			return this;
		}

		@Nonnull
		public Builder withFetchIntervalMillis(final long fetchIntervalMillis) {
			this.fetchIntervalMillis = fetchIntervalMillis;
			return this;
		}

		@Nonnull
		public Builder withMaxNumberOfRecordsPerFetch(final int maxNumberOfRecordsPerFetch) {
			this.maxNumberOfRecordsPerFetch = maxNumberOfRecordsPerFetch;
			return this;
		}

		@Nonnull
		public PollingRecordPublisher build() {
			return new PollingRecordPublisher(this);
		}
	}
}
