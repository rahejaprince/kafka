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
package kafka.metrics;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import io.opentelemetry.proto.resource.v1.Resource;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.server.telemetry.ClientTelemetry;
import org.apache.kafka.server.telemetry.ClientTelemetryPayload;
import org.apache.kafka.server.telemetry.ClientTelemetryReceiver;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;


/**
 * MetricsReporter that aggregates Opentelemetry Protocol(OTLP) metrics, enhance them with additional client labels
 * and forwards them via gRPC Client to an external OTLP receiver.
 */
public class OtlpMetricsReporter implements MetricsReporter, ClientTelemetry {

    private static final Logger log = LoggerFactory.getLogger(OtlpMetricsReporter.class);

    private static final String CLIENT_LABELS_CONFIG = "confluent.telemetry.clientMetrics.appendClientLabels";

    private static final String GRPC_ENDPOINT_CONFIG = "OTEL_EXPORTER_OTLP_ENDPOINT";

    private static final int DEFAULT_GRPC_PORT = 4317;

    private boolean appendClientLabels = true;

    private final MetricsGrpcClient grpcService;

    // Kafka-specific labels
    private Map<String, String> metricsContext;
    private static final String KAFKA_BROKER_ID = "kafka.broker.id";
    private static final String KAFKA_CLUSTER_ID = "kafka.cluster.id";


    /**
     * Initializes the MetricsReporter with a {@link MetricsGrpcClient} at an endpoint defined by the
     * {@value GRPC_ENDPOINT_CONFIG} environment variable
     */
    public OtlpMetricsReporter() {
        String grpcEndpoint = System.getenv(GRPC_ENDPOINT_CONFIG);

        if (Utils.isBlank(grpcEndpoint)) {
            log.info("environment variable {} is not set", GRPC_ENDPOINT_CONFIG);
            try {
                grpcEndpoint = InetAddress.getLocalHost().getHostAddress() + ":" + DEFAULT_GRPC_PORT;
            } catch (UnknownHostException e) {
                log.warn("Failed to get local host address: {}", e.getMessage());
            }
        }

        ManagedChannel grpcChannel = ManagedChannelBuilder.forTarget(grpcEndpoint).usePlaintext().build();

        grpcService = new MetricsGrpcClient(grpcChannel);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Object clientLabelsConfig = configs.get(CLIENT_LABELS_CONFIG);
        if (clientLabelsConfig != null) {
            appendClientLabels = (boolean) clientLabelsConfig;
        }
    }


    @Override
    public void contextChange(MetricsContext metricsContext) {
        this.metricsContext = metricsContext.contextLabels();
    }


    @Override
    public void close() {
        grpcService.close();
    }

    @Override
    public ClientTelemetryReceiver clientReceiver() {
        return (context, payload) -> {
            try {
                log.info("Exporting metrics. Context: {}, payload: {}", context, payload);

                if (payload == null || payload.data() == null) {
                    log.warn("exportMetrics - Client did not include payload when pushing to broker, skipping export");
                    return;
                }

                log.info("Metrics data compressed size: {}", payload.data().remaining());
                MetricsData metricsData = deserializeMetricsData(payload.data());
                if (metricsData == null || metricsData.getResourceMetricsCount() == 0) {
                    log.warn("exportMetrics - No metrics available, skipping export");
                    return;
                }

//                log.info("Pre-processed metrics data: {}, count: {}, size: {}", metricsData, metricsData.getResourceMetricsCount(),
//                    metricsData.getSerializedSize());

                List<ResourceMetrics> metrics = metricsData.getResourceMetricsList();
//                log.info("Metrics: {}", metrics);
                // enhance metrics with Broker-added labels
                if (appendClientLabels) {
                    Map<String, String> labels = getClientLabels(context, payload);
                    metrics = enhanceMetrics(metrics, labels);
                }

//                log.info("Post-processed metrics: {}, count: {}, size: {}", metrics, metricsData.getResourceMetricsCount(),
//                    metricsData.getSerializedSize());

                log.info("[APM] sending metrics to client");
                grpcService.export(metrics);
            } catch (Exception e) {
                log.error("Error: ", e);
            }
        };
    }

    private static MetricsData deserializeMetricsData(ByteBuffer serializedMetricsData) {
        try {
//            ByteBuffer duplicate = (ByteBuffer) serializedMetricsData.flip();
            return MetricsData.parseFrom(serializedMetricsData);
        } catch (IOException e) {
            throw new KafkaException("Unable to parse MetricsData payload", e);
        }
    }


    /**
     * Returns a list of ResourceMetrics where each metrics is enhanced by adding client information as
     * additional resource level attributes
     * @param resourceMetrics resource metrics sent by client
     * @param labels broker added labels containing client information
     * @return enhanced ResourceMetrics list
     */
    public List<ResourceMetrics> enhanceMetrics(List<ResourceMetrics> resourceMetrics,
                                                Map<String, String> labels) {

        List<ResourceMetrics> updatedResourceMetrics = new ArrayList<>();

        resourceMetrics.forEach(rm -> {
            Resource.Builder resource = rm.getResource().toBuilder();

            labels.forEach((k, v) -> resource.addAttributes(
                    KeyValue.newBuilder()
                            .setKey(k)
                            .setValue(AnyValue.newBuilder().setStringValue(v))
                            .build()
                    ));

            ResourceMetrics updatedMetric = rm.toBuilder()
                    .setResource(resource.build())
                    .build();

            updatedResourceMetrics.add(updatedMetric);
        });

        return updatedResourceMetrics;
    }

    public static class ClientLabels {

        public static final String CLIENT_ID = "client_id";
        public static final String CLIENT_INSTANCE_ID = "client_instance_id";
        public static final String CLIENT_SOFTWARE_NAME = "client_software_name";
        public static final String CLIENT_SOFTWARE_VERSION = "client_software_version";
        public static final String CLIENT_SOURCE_ADDRESS = "client_source_address";
        public static final String CLIENT_SOURCE_PORT = "client_source_port";
        public static final String PRINCIPAL = "principal";
    }

    public Map<String, String> getClientLabels(AuthorizableRequestContext context, ClientTelemetryPayload payload) {
        RequestContext requestContext = (RequestContext) context;

        Map<String, String> labels = new HashMap<>();
        putIfNotNull(labels, ClientLabels.CLIENT_ID, context.clientId());
        putIfNotNull(labels, ClientLabels.CLIENT_INSTANCE_ID, payload.clientInstanceId().toString());
        putIfNotNull(labels, ClientLabels.CLIENT_SOFTWARE_NAME, requestContext.clientInformation.softwareName());
        putIfNotNull(labels, ClientLabels.CLIENT_SOFTWARE_VERSION, requestContext.clientInformation.softwareVersion());
        putIfNotNull(labels, ClientLabels.CLIENT_SOURCE_ADDRESS, requestContext.clientAddress().getHostAddress());
        putIfNotNull(labels, ClientLabels.CLIENT_SOURCE_PORT, Integer.toString(requestContext.clientPort.orElse(-1)));
        putIfNotNull(labels, ClientLabels.PRINCIPAL, requestContext.principal().getName());


        // Include Kafka cluster and broker id from the MetricsContext
        putIfNotNull(labels, KAFKA_CLUSTER_ID, metricsContext.get(KAFKA_CLUSTER_ID));
        putIfNotNull(labels, KAFKA_BROKER_ID, metricsContext.get(KAFKA_BROKER_ID));

        return labels;
    }

    public static void putIfNotNull(Map<String, String> map, String key, String value) {
        Optional.ofNullable(value).ifPresent(v -> map.put(key, v));
    }

    @Override
    public void init(List<KafkaMetric> metrics) {}

    @Override
    public void metricChange(KafkaMetric metric) {}

    @Override
    public void metricRemoval(KafkaMetric metric) {}

}
