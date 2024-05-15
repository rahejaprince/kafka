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

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.grpc.stub.StreamObserver;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;
import java.util.List;


class MetricsGrpcClient implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(MetricsGrpcClient.class);

  private final StreamObserver<ExportMetricsServiceResponse> streamObserver =
      new StreamObserver<ExportMetricsServiceResponse>() {

        @Override
        public void onNext(ExportMetricsServiceResponse value) {
          // Do nothing since response is blank
        }

        @Override
        public void onError(Throwable t) {
          log.error("Unable to export metrics request to {}. gRPC Connectivity in: {}:  {}",
              endpoint, getChannelState(), t.getCause());
        }

        @Override
        public void onCompleted() {
          log.info("Successfully exported metrics request to {}", endpoint);
        }
      };

  private static final int GRPC_CHANNEL_TIMEOUT = 30;

  private final MetricsServiceGrpc.MetricsServiceStub grpcClient;

  private ManagedChannel grpcChannel;

  private final String endpoint;


  /**
   * Starts a {@link MetricsServiceGrpc} at with a given gRPC {@link ManagedChannel}
   */
  public MetricsGrpcClient(ManagedChannel channel) {
    grpcChannel = channel;
    grpcClient = MetricsServiceGrpc.newStub(channel);

    endpoint = grpcChannel.authority();
    log.info("Started gRPC Client for endpoint  {}", endpoint);
  }


  /**
   * Exports a ExportMetricsServiceRequest to an external endpoint
   * @param resourceMetrics metrics to export
   */
  public void export(List<ResourceMetrics> resourceMetrics) {
    ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
        .addAllResourceMetrics(resourceMetrics)
        .build();

    grpcClient.export(request, streamObserver);
  }


  /**
   * Shuts down the gRPC {@link ManagedChannel}
   */
  @Override
  public void close()  {
    try {
      log.info("Shutting down gRPC channel at {}", endpoint);
      grpcChannel.shutdown().awaitTermination(GRPC_CHANNEL_TIMEOUT, TimeUnit.SECONDS);
    } catch (Exception e) {
      log.warn("Failed Shutting down gRPC channel at {} : {}", endpoint, e.getMessage());
    }
  }


  public ConnectivityState getChannelState() {
    return grpcChannel.getState(true);
  }

  // Visible for testing
  String getEndpoint() {
    return endpoint;
  }

  // Visible for testing
  ManagedChannel getGrpcChannel() {
    return grpcChannel;
  }
}
