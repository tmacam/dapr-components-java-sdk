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

import dapr.proto.components.v1.Bindings;
import dapr.proto.components.v1.InputBindingGrpc;
import io.dapr.components.domain.bindings.InputBinding;
import io.dapr.v1.ComponentProtos;
import io.grpc.stub.StreamObserver;
import reactor.core.publisher.Mono;

import java.util.Objects;

public class InputBindingGrpcComponentWrapper extends InputBindingGrpc.InputBindingImplBase {

  private final InputBinding inputBinding;

  public InputBindingGrpcComponentWrapper(InputBinding inputBinding) {
    this.inputBinding = Objects.requireNonNull(inputBinding);
  }

  @Override
  public void init(Bindings.InputBindingInitRequest request, StreamObserver<Bindings.InputBindingInitResponse> responseObserver) {
    Mono.just(request)
        .flatMap(req -> inputBinding.init(req.getMetadata().getPropertiesMap()))
        // Response is functionally and structurally equivalent to Empty, nothing to fill.
        .map(response -> Bindings.InputBindingInitResponse.getDefaultInstance())
        .subscribe(responseObserver::onNext, responseObserver::onError, responseObserver::onCompleted);
  }

  @Override
  public void ping(ComponentProtos.PingRequest request, StreamObserver<ComponentProtos.PingResponse> responseObserver) {
    Mono.just(request)
        .flatMap(req -> inputBinding.ping())
        // Response is functionally and structurally equivalent to Empty, nothing to fill.
        .map(successfulPing -> ComponentProtos.PingResponse.getDefaultInstance())
        .subscribe(responseObserver::onNext, responseObserver::onError, responseObserver::onCompleted);
  }

  @Override
  public StreamObserver<Bindings.ReadRequest> read(StreamObserver<Bindings.ReadResponse> responseObserver) {
    // TODO
    return super.read(responseObserver);
  }
}
