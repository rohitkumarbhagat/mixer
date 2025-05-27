// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package interceptor provides gRPC interceptors.
package interceptor

import (
	"context"
	"encoding/json"
	"log"

	"google.golang.org/grpc"
)

// LoggingInterceptor is a gRPC unary server interceptor that logs the request payload.
func LoggingInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	resp, err := handler(ctx, req)
	if err != nil {
		return resp, err
	}

	// We only care about successful non-streaming RPCs.
	// TODO(LS): Add check for streaming RPCs if necessary. For now, this is a unary interceptor.

	payload, err := json.Marshal(req)
	if err != nil {
		log.Printf("Failed to marshal request payload to JSON: %v", err)
		return resp, nil // Return success even if logging fails.
	}

	log.Printf("Request payload for method %s: %s", info.FullMethod, string(payload))
	return resp, nil
}
