/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.telemetry;

import static org.apache.kafka.clients.telemetry.ClientTelemetryUtils.validateAcceptedCompressionTypes;
import static org.apache.kafka.clients.telemetry.ClientTelemetryUtils.validateClientInstanceId;
import static org.apache.kafka.clients.telemetry.ClientTelemetryUtils.validateMetricNames;
import static org.apache.kafka.clients.telemetry.ClientTelemetryUtils.validatePushIntervalMs;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData;
import org.apache.kafka.common.message.PushTelemetryResponseData;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultClientTelemetry implements ClientTelemetry {

    private static final Logger log = LoggerFactory.getLogger(DefaultClientTelemetry.class);

    private static final String CONTEXT = "kafka.telemetry";

    private static final String CLIENT_ID_METRIC_TAG = "client-id";

    private final Time time;

    private final Metrics metrics;

    private final DeltaValueStore deltaValueStore;

    private final TelemetryMetricsReporter telemetryMetricsReporter;

    private final TelemetrySerializer telemetrySerializer;

    private final ReadWriteLock subscriptionLock = new ReentrantReadWriteLock();

    private final Condition subscriptionLoaded = subscriptionLock.writeLock().newCondition();

    private TelemetrySubscription subscription;

    private final ReadWriteLock stateLock = new ReentrantReadWriteLock();

    private final Condition terminalPushInProgress = stateLock.writeLock().newCondition();

    private TelemetryState state = TelemetryState.subscription_needed;

    private final ClientInstanceMetricRecorder clientInstanceMetricRecorder;

    private final ConsumerMetricRecorder consumerMetricRecorder;

    private final HostProcessMetricRecorder hostProcessMetricRecorder;

    private final ProducerMetricRecorder producerMetricRecorder;

    private final ProducerTopicMetricRecorder producerTopicMetricRecorder;

    public DefaultClientTelemetry(Time time, String clientId) {
        if (time == null)
            throw new IllegalArgumentException("time for ClientTelemetryImpl cannot be null");

        if (clientId == null)
            throw new IllegalArgumentException("clientId for ClientTelemetryImpl cannot be null");

        this.time = Objects.requireNonNull(time, "time must be non-null");
        this.telemetrySerializer = new OtlpTelemetrySerializer();
        this.deltaValueStore = new DeltaValueStore();
        this.telemetryMetricsReporter = new TelemetryMetricsReporter(deltaValueStore);

        Map<String, String> metricsTags = Collections.singletonMap(CLIENT_ID_METRIC_TAG, clientId);
        MetricConfig metricConfig = new MetricConfig()
            .tags(metricsTags);
        MetricsContext metricsContext = new KafkaMetricsContext(CONTEXT);

        this.metrics = new Metrics(metricConfig,
            Collections.singletonList(telemetryMetricsReporter),
            time,
            metricsContext);

        this.clientInstanceMetricRecorder = new DefaultClientInstanceMetricRecorder(this.metrics);
        this.consumerMetricRecorder = new DefaultConsumerMetricRecorder(this.metrics);
        this.hostProcessMetricRecorder = new DefaultHostProcessMetricRecorder(this.metrics);
        this.producerMetricRecorder = new DefaultProducerMetricRecorder(this.metrics);
        this.producerTopicMetricRecorder = new DefaultProducerTopicMetricRecorder(this.metrics);
    }

    @Override
    public void initiateClose(Duration timeout) {
        log.trace("initiateClose");

        long timeoutMs = timeout.toMillis();

        if (timeoutMs < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");

        // If we never had a subscription, we can't really push anything.
        if (!subscription().isPresent()) {
            log.debug("Telemetry subscription not loaded, not attempting terminating push");
            return;
        }

        try {
            setState(TelemetryState.terminating_push_needed);
        } catch (IllegalTelemetryStateException e) {
            log.warn("Error initiating client telemetry close", e);
        }

        try {
            stateLock.readLock().unlock();

            try {
                if (!terminalPushInProgress.await(timeoutMs, TimeUnit.MILLISECONDS))
                    log.debug("Wait for terminal telemetry push to be submitted has elapsed; may not have actually sent request");
            } catch (InterruptedException e) {
                throw new InterruptException(e);
            }
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public void close() {
        log.trace("close");

        try {
            stateLock.writeLock().lock();
            TelemetryState newState = TelemetryState.terminated;

            if (state != newState) {
                try {
                    // This *shouldn't* throw an exception, but let's wrap it anyway so that we're
                    // sure to close the metrics object.
                    setState(TelemetryState.terminated);
                } finally {
                    metrics.close();
                }
            } else {
                log.debug("Ignoring subsequent {} close", ClientTelemetry.class.getSimpleName());
            }
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    private void setSubscription(TelemetrySubscription newSubscription) {
        try {
            subscriptionLock.writeLock().lock();

            log.trace("Setting subscription from {} to {}", this.subscription, newSubscription);
            this.subscription = newSubscription;

            // In some cases we have to wait for this signal in the clientInstanceId method so that
            // we know that we have a subscription to pull from.
            subscriptionLoaded.signalAll();
        } finally {
            subscriptionLock.writeLock().unlock();
        }
    }

    public Optional<TelemetrySubscription> subscription() {
        return Optional.ofNullable(subscriptionInternal());
    }

    public TelemetrySubscription subscriptionInternal() {
        try {
            subscriptionLock.readLock().lock();
            return subscription;
        } finally {
            subscriptionLock.readLock().unlock();
        }
    }

    /**
     * Determines the client's unique client instance ID used for telemetry. This ID is unique to
     * the specific enclosing client instance and will not change after it is initially generated.
     * The ID is useful for correlating client operations with telemetry sent to the broker and
     * to its eventual monitoring destination(s).
     *
     * This method waits up to <code>timeout</code> for the subscription to become available in
     * order to complete the request.
     *
     * @param timeout The maximum time to wait for enclosing client instance to determine its
     *                client instance ID. The value should be non-negative. Specifying a timeout
     *                of zero means do not wait for the initial request to complete if it hasn't
     *                already.
     * @throws InterruptException If the thread is interrupted while blocked.
     * @throws KafkaException If an unexpected error occurs while trying to determine the client
     *                        instance ID, though this error does not necessarily imply the
     *                        enclosing client instance is otherwise unusable.
     * @throws IllegalArgumentException If the <code>timeout</code> is negative.
     * @return Human-readable string representation of the client instance ID
     */
    @Override
    public Optional<String> clientInstanceId(Duration timeout) {
        log.trace("clientInstanceId");

        long timeoutMs = timeout.toMillis();

        if (timeoutMs < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");

        TelemetryState state = stateInternal();

        try {
            subscriptionLock.readLock().lock();

            // We can use the instance variable directly here because we're handling the locking...
            if (subscription == null) {
                // If we have a non-negative timeout and no-subscription, let's wait for one to
                // be retrieved.
                log.trace("Waiting for telemetry subscription containing the client instance ID with timeoutMillis = {} ms.", timeoutMs);

                try {
                    if (!subscriptionLoaded.await(timeoutMs, TimeUnit.MILLISECONDS))
                        log.debug("Wait for telemetry subscription elapsed; may not have actually loaded it");
                } catch (InterruptedException e) {
                    throw new InterruptException(e);
                }
            }

            if (subscription == null) {
                log.debug("Client instance ID could not be retrieved with timeout {}", timeout);
                return Optional.empty();
            }

            Uuid clientInstanceId = subscription.clientInstanceId();

            if (clientInstanceId == null) {
                log.debug("Client instance ID was null in telemetry subscription while in state {}", state);
                return Optional.empty();
            }

            return Optional.of(clientInstanceId.toString());
        } finally {
            subscriptionLock.readLock().unlock();
        }
    }

    @Override
    public void setState(TelemetryState newState) {
        try {
            stateLock.writeLock().lock();
            this.state = this.state.validateTransition(newState);
            log.trace("Set telemetry state from {} to {}", this.state, newState);

            if (newState == TelemetryState.terminating_push_in_progress) {
                terminalPushInProgress.signalAll();
                log.debug("Wait for terminal telemetry push elapsed; may not have actually sent request");
            }
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    @Override
    public Optional<TelemetryState> state() {
        return Optional.of(stateInternal());
    }

    private TelemetryState stateInternal() {
        try {
            stateLock.readLock().lock();
            return state;
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public void telemetrySubscriptionFailed(Throwable error) {
        if (error != null)
            log.warn("Failed to retrieve telemetry subscription; using existing subscription", error);
        else
            log.warn("Failed to retrieve telemetry subscription; using existing subscription", new Exception());

        setState(TelemetryState.subscription_needed);
    }

    @Override
    public void pushTelemetryFailed(Throwable error) {
        if (error != null)
            log.warn("Failed to push telemetry", error);
        else
            log.warn("Failed to push telemetry", new Exception());

        TelemetryState state = stateInternal();

        if (state == TelemetryState.push_in_progress)
            setState(TelemetryState.subscription_needed);
        else if (state == TelemetryState.terminating_push_in_progress)
            setState(TelemetryState.terminated);
        else
            throw new IllegalTelemetryStateException(String.format("Could not transition state after failed push telemetry from state %s", state));
    }

    @Override
    public void telemetrySubscriptionSucceeded(GetTelemetrySubscriptionsResponseData data) {
        List<String> requestedMetrics = data.requestedMetrics();

        // TODO: TELEMETRY_TODO: this is temporary until we get real data back from broker...
        requestedMetrics.add("");

        MetricSelector metricSelector = validateMetricNames(requestedMetrics);
        List<CompressionType> acceptedCompressionTypes = validateAcceptedCompressionTypes(data.acceptedCompressionTypes());
        Uuid clientInstanceId = validateClientInstanceId(data.clientInstanceId());
        // TODO: TELEMETRY_TODO: this is temporary until we get real data back from broker...
        int pushIntervalMs = validatePushIntervalMs(data.pushIntervalMs() > 0 ? data.pushIntervalMs() : 10000);

        TelemetrySubscription telemetrySubscription = new TelemetrySubscription(time.milliseconds(),
            data.throttleTimeMs(),
            clientInstanceId,
            data.subscriptionId(),
            acceptedCompressionTypes,
            pushIntervalMs,
            data.deltaTemporality(),
            metricSelector);

        log.debug("Successfully retrieved telemetry subscription: {}", telemetrySubscription);
        setSubscription(telemetrySubscription);

        if (metricSelector == MetricSelector.NONE) {
            // This is the case where no metrics are requested and/or match the filters. We need
            // to wait pushIntervalMs then retry.
            setState(TelemetryState.subscription_needed);
        } else {
            setState(TelemetryState.push_needed);
        }
    }

    @Override
    public void pushTelemetrySucceeded(PushTelemetryResponseData data) {
        log.debug("Successfully pushed telemetry; response: {}", data);

        TelemetryState state = stateInternal();

        if (state == TelemetryState.push_in_progress)
            setState(TelemetryState.subscription_needed);
        else if (state == TelemetryState.terminating_push_in_progress)
            setState(TelemetryState.terminated);
        else
            throw new IllegalTelemetryStateException(String.format("Could not transition state after successful push telemetry from state %s", state));
    }

    @Override
    public Optional<Long> timeToNextUpdate(long requestTimeoutMs) {
        TelemetrySubscription subscription = subscriptionInternal();
        TelemetryState state = stateInternal();
        long t = ClientTelemetryUtils.timeToNextUpdate(state, subscription, requestTimeoutMs, time);
        log.trace("For telemetry state {}, returning {} for time to next telemetry update", state, t);
        return Optional.of(t);
    }

    @Override
    public Optional<AbstractRequest.Builder<?>> createRequest() {
        return ClientTelemetryUtils.createRequest(stateInternal(),
            subscription().orElse(null),
            telemetrySerializer,
            telemetryMetricsReporter.current(),
            deltaValueStore,
            this::setState);
    }

    @Override
    public ClientInstanceMetricRecorder clientInstanceMetricRecorder() {
        return clientInstanceMetricRecorder;
    }

    @Override
    public ConsumerMetricRecorder consumerMetricRecorder() {
        return consumerMetricRecorder;
    }

    @Override
    public HostProcessMetricRecorder hostProcessMetricRecorder() {
        return hostProcessMetricRecorder;
    }

    @Override
    public ProducerMetricRecorder producerMetricRecorder() {
        return producerMetricRecorder;
    }

    @Override
    public ProducerTopicMetricRecorder producerTopicMetricRecorder() {
        return producerTopicMetricRecorder;
    }
}
