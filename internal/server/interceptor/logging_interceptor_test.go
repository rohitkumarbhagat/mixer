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

package interceptor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	pb "github.com/datacommonsorg/mixer/internal/proto/service" // Using an existing proto for simplicity
)

// TestMain is used to capture log output.
var logBuffer bytes.Buffer

func TestMain(m *testing.M) {
	log.SetOutput(&logBuffer)
	exitCode := m.Run()
	log.SetOutput(os.Stderr) // Restore original logger
	os.Exit(exitCode)
}

func TestLoggingInterceptor(t *testing.T) {
	ctx := context.Background()
	info := &grpc.UnaryServerInfo{
		FullMethod: "/test.service/TestMethod",
	}

	// Test case 1: Successful RPC
	t.Run("SuccessfulRPC", func(t *testing.T) {
		logBuffer.Reset()
		req := &pb.GetObservedVariableRequest{ // Using an existing simple proto message
			Entity: "test_entity",
			Variable: "test_variable",
		}
		expectedResp := &pb.GetObservedVariableResponse{ // Dummy response
			Places: []string{"place1", "place2"},
		}
		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			return expectedResp, nil
		}

		resp, err := LoggingInterceptor(ctx, req, info, handler)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if !proto.Equal(resp.(proto.Message), expectedResp) {
			t.Errorf("Expected response %v, got %v", expectedResp, resp)
		}

		loggedOutput := logBuffer.String()
		reqJSON, _ := json.Marshal(req)
		expectedLog := "Request payload for method /test.service/TestMethod: " + string(reqJSON)

		if !strings.Contains(loggedOutput, expectedLog) {
			t.Errorf("Expected log to contain '%s', got '%s'", expectedLog, loggedOutput)
		}
	})

	// Test case 2: Failed RPC (handler returns an error)
	t.Run("FailedRPC", func(t *testing.T) {
		logBuffer.Reset()
		req := &pb.GetObservedVariableRequest{
			Entity: "test_entity_fail",
			Variable: "test_variable_fail",
		}
		expectedErr := errors.New("handler error")
		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			return nil, expectedErr
		}

		_, err := LoggingInterceptor(ctx, req, info, handler)

		if err == nil {
			t.Errorf("Expected an error, got nil")
		}
		if err != expectedErr {
			t.Errorf("Expected error %v, got %v", expectedErr, err)
		}

		loggedOutput := logBuffer.String()
		// Expect no "Request payload" log entry for failed RPCs
		if strings.Contains(loggedOutput, "Request payload for method") {
			t.Errorf("Expected no request payload log for failed RPC, got '%s'", loggedOutput)
		}
	})

	// Test case 3: JSON Marshalling failure (should still succeed RPC)
	t.Run("JSONMarshalFailure", func(t *testing.T) {
		logBuffer.Reset()
		// Create a request that will cause json.Marshal to fail.
		// json.Marshal fails on channels, functions, and complex numbers.
		// Using a channel here.
		req := struct {
			BadField chan int
		}{
			BadField: make(chan int),
		}

		expectedResp := "success_response" // Dummy response
		handler := func(ctx context.Context, r interface{}) (interface{}, error) {
			// Ensure the request received by the handler is the one we sent
			if r != req {
				t.Errorf("Handler received unexpected request: got %v, want %v", r, req)
			}
			return expectedResp, nil
		}

		resp, err := LoggingInterceptor(ctx, req, info, handler)

		if err != nil {
			t.Errorf("Expected no error from interceptor despite marshal failure, got %v", err)
		}
		if resp != expectedResp {
			t.Errorf("Expected response %v, got %v", expectedResp, resp)
		}

		loggedOutput := logBuffer.String()
		expectedLog := "Failed to marshal request payload to JSON"
		if !strings.Contains(loggedOutput, expectedLog) {
			t.Errorf("Expected log to contain marshal failure message '%s', got '%s'", expectedLog, loggedOutput)
		}
		// Ensure the actual payload wasn't logged
		if strings.Contains(loggedOutput, "Request payload for method") {
			t.Errorf("Expected no request payload log due to marshal failure, got '%s'", loggedOutput)
		}
	})
}
