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
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.function.Consumer;

/**
 * A proxy record publisher to introduce a dynamic loop delay
 * and batch read size for {@link PollingRecordPublisher}.
 */
@Internal
public class AdaptivePollingRecordPublisher implements RecordPublisher {
	// AWS Kinesis has a read limit of 2 Mb/sec
	// https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html
	private static final long KINESIS_SHARD_BYTES_PER_SECOND_LIMIT = 2 * 1024L * 1024L;

	private final PollingRecordPublisher pollingRecordPublisher;

	private final PollingRecordPublisherMetricsReporter metricsReporter;

	private int maxNumberOfRecordsPerFetch;

	private final long fetchIntervalMillis;

	private int lastRecordBatchSize = 0;

	private long lastRecordBatchSizeInBytes = 0;

	private long processingStartTimeNanos = System.nanoTime();

	private AdaptivePollingRecordPublisher(@Nonnull final Builder builder) {
		this.pollingRecordPublisher = builder.pollingRecordPublisher;
		this.metricsReporter = builder.metricsReporter;
		this.fetchIntervalMillis = builder.fetchIntervalMillis;
		this.maxNumberOfRecordsPerFetch = builder.maxNumberOfRecordsPerFetch;
	}

	@Override
	public void subscribe(
			@Nonnull final StartingPosition startingPosition,
			@Nonnull final Consumer<RecordBatch> consumer) throws InterruptedException {
		this.pollingRecordPublisher.subscribe(startingPosition, batch -> {
			Preconditions.checkNotNull(consumer).accept(batch);
			lastRecordBatchSize = batch.getDeaggregatedRecordSize();
			lastRecordBatchSizeInBytes = batch.getTotalSizeInBytes();
		});
	}

	@Override
	public void run(@Nonnull final SequenceNumber lastSequenceNumber) throws InterruptedException {
		pollingRecordPublisher.run(lastSequenceNumber, maxNumberOfRecordsPerFetch);

		long adjustmentEndTimeNanos = adjustRunLoopFrequency(processingStartTimeNanos, System.nanoTime());
		long runLoopTimeNanos = adjustmentEndTimeNanos - processingStartTimeNanos;
		maxNumberOfRecordsPerFetch = adaptRecordsToRead(runLoopTimeNanos, lastRecordBatchSize, lastRecordBatchSizeInBytes, maxNumberOfRecordsPerFetch);
		metricsReporter.setRunLoopTimeNanos(runLoopTimeNanos);
		processingStartTimeNanos = adjustmentEndTimeNanos;
	}

	@Override
	public boolean isComplete() {
		return pollingRecordPublisher.isComplete();
	}

	/**
	 * Adjusts loop timing to match target frequency if specified.
	 * @param processingStartTimeNanos The start time of the run loop "work"
	 * @param processingEndTimeNanos The end time of the run loop "work"
	 * @return The System.nanoTime() after the sleep (if any)
	 * @throws InterruptedException
	 */
	private long adjustRunLoopFrequency(long processingStartTimeNanos, long processingEndTimeNanos)
			throws InterruptedException {
		long endTimeNanos = processingEndTimeNanos;
		if (fetchIntervalMillis != 0) {
			long processingTimeNanos = processingEndTimeNanos - processingStartTimeNanos;
			long sleepTimeMillis = fetchIntervalMillis - (processingTimeNanos / 1_000_000);
			if (sleepTimeMillis > 0) {
				Thread.sleep(sleepTimeMillis);
				endTimeNanos = System.nanoTime();
				metricsReporter.setSleepTimeMillis(sleepTimeMillis);
			}
		}
		return endTimeNanos;
	}

	/**
	 * Calculates how many records to read each time through the loop based on a target throughput
	 * and the measured frequenecy of the loop.
	 * @param runLoopTimeNanos The total time of one pass through the loop
	 * @param numRecords The number of records of the last read operation
	 * @param recordBatchSizeBytes The total batch size of the last read operation
	 * @param maxNumberOfRecordsPerFetch The current maxNumberOfRecordsPerFetch
	 */
	private int adaptRecordsToRead(long runLoopTimeNanos, int numRecords, long recordBatchSizeBytes,
			int maxNumberOfRecordsPerFetch) {
		if (numRecords != 0 && runLoopTimeNanos != 0) {
			long averageRecordSizeBytes = recordBatchSizeBytes / numRecords;
			// Adjust number of records to fetch from the shard depending on current average record size
			// to optimize 2 Mb / sec read limits
			double loopFrequencyHz = 1000000000.0d / runLoopTimeNanos;
			double bytesPerRead = KINESIS_SHARD_BYTES_PER_SECOND_LIMIT / loopFrequencyHz;
			maxNumberOfRecordsPerFetch = (int) (bytesPerRead / averageRecordSizeBytes);
			// Ensure the value is greater than 0 and not more than 10000L
			maxNumberOfRecordsPerFetch = Math.max(1, Math.min(maxNumberOfRecordsPerFetch, ConsumerConfigConstants.DEFAULT_SHARD_GETRECORDS_MAX));

			// Set metrics
			metricsReporter.setLoopFrequencyHz(loopFrequencyHz);
			metricsReporter.setBytesPerRead(bytesPerRead);
		}
		return maxNumberOfRecordsPerFetch;
	}

	@Nonnull
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * A builder used to build immutable instances of {@link AdaptivePollingRecordPublisher} using a builder pattern.
	 */
	public static class Builder {

		private PollingRecordPublisher pollingRecordPublisher;

		private PollingRecordPublisherMetricsReporter metricsReporter;

		private long fetchIntervalMillis = ConsumerConfigConstants.DEFAULT_SHARD_GETRECORDS_INTERVAL_MILLIS;

		private int maxNumberOfRecordsPerFetch = ConsumerConfigConstants.DEFAULT_SHARD_GETRECORDS_MAX;

		@Nonnull
		public Builder withPollingRecordPublisher(@Nonnull final PollingRecordPublisher pollingRecordPublisher) {
			this.pollingRecordPublisher = pollingRecordPublisher;
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
		public AdaptivePollingRecordPublisher build() {
			return new AdaptivePollingRecordPublisher(this);
		}
	}
}
