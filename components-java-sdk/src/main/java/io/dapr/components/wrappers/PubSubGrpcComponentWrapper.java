/*
 * Copyright 2023 The Dapr Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dapr.components.wrappers;

import dapr.proto.components.v1.PubSubGrpc;
import dapr.proto.components.v1.Pubsub;
import io.dapr.components.domain.pubsub.PubSub;
import io.dapr.v1.ComponentProtos;
import io.grpc.stub.StreamObserver;
import reactor.core.publisher.Mono;

import java.util.Objects;

public class PubSubGrpcComponentWrapper extends PubSubGrpc.PubSubImplBase {

  private final PubSub pubSub;

  /**
   * Constructor.
   *
   * @param pubSub the pubsub that this component will expose as a service.
   */
  public PubSubGrpcComponentWrapper(PubSub pubSub) {
    this.pubSub = Objects.requireNonNull(pubSub);
  }

  @Override
  public void init(Pubsub.PubSubInitRequest request, StreamObserver<Pubsub.PubSubInitResponse> responseObserver) {
    Mono.just(request)
        .flatMap(req -> pubSub.init(req.getMetadata().getPropertiesMap()))
        // Response is functionally and structurally equivalent to Empty, nothing to fill.
        .map(response -> Pubsub.PubSubInitResponse.getDefaultInstance())
        .subscribe(responseObserver::onNext, responseObserver::onError, responseObserver::onCompleted);
  }

  @Override
  public void features(ComponentProtos.FeaturesRequest request, StreamObserver<ComponentProtos.FeaturesResponse> responseObserver) {
    Mono.just(request)
        .flatMap(req -> pubSub.getFeatures())
        .map(features -> ComponentProtos.FeaturesResponse.newBuilder()
            .addAllFeatures(features)
            .build()
        )
        .subscribe(responseObserver::onNext, responseObserver::onError, responseObserver::onCompleted);
  }

  @Override
  public void ping(ComponentProtos.PingRequest request, StreamObserver<ComponentProtos.PingResponse> responseObserver) {
    Mono.just(request)
        .flatMap(req -> pubSub.ping())
        // Response is functionally and structurally equivalent to Empty, nothing to fill.
        .map(successfulPing -> ComponentProtos.PingResponse.getDefaultInstance())
        .subscribe(responseObserver::onNext, responseObserver::onError, responseObserver::onCompleted);
  }

  @Override
  public void publish(Pubsub.PublishRequest request, StreamObserver<Pubsub.PublishResponse> responseObserver) {
    super.publish(request, responseObserver);
  }

  @Override
  public StreamObserver<Pubsub.PullMessagesRequest> pullMessages(StreamObserver<Pubsub.PullMessagesResponse> responseObserver) {
    // TODO
    return super.pullMessages(responseObserver);
  }

}
