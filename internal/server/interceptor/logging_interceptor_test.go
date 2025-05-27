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
	"math/rand"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	pb "github.com/datacommonsorg/mixer/internal/proto/service" // Using an existing proto
)

var logBuffer bytes.Buffer

// mockRandSource is a mock implementation of rand.Source for deterministic testing.
type mockRandSource struct {
	Value int64 // The value to be returned by Int63(), which rand.Intn uses.
}

func (s *mockRandSource) Int63() int64 {
	return s.Value
}

func (s *mockRandSource) Seed(seed int64) {
	// No-op for this mock
}

// mockRandSourceAdvanced allows a sequence of return values.
type mockRandSourceAdvanced struct {
	Values []int64
	idx    int32
}

func newMockRandSourceAdvanced(values []int64) *mockRandSourceAdvanced {
	return &mockRandSourceAdvanced{Values: values}
}

func (s *mockRandSourceAdvanced) Int63() int64 {
	// Atomically load and increment idx to ensure thread-safety if tests were parallel.
	// For t.Run, subtests run sequentially by default unless t.Parallel() is called.
	currentIdx := atomic.LoadInt32(&s.idx)
	val := s.Values[currentIdx%int32(len(s.Values))]
	atomic.AddInt32(&s.idx, 1)
	return val
}

func (s *mockRandSourceAdvanced) Seed(seed int64) {
	// No-op
}

func TestMain(m *testing.M) {
	originalLogOutput := log.Writer()
	log.SetOutput(&logBuffer) // Redirect global log output to our buffer

	// Run all tests
	exitCode := m.Run()

	// Restore original log output
	log.SetOutput(originalLogOutput)
	// Reset logging percentage and random source to defaults after all tests
	InitializeForTest(100, rand.NewSource(time.Now().UnixNano()))

	os.Exit(exitCode)
}

// Helper function to create a basic successful handler
func successfulHandler(expectedResp proto.Message) grpc.UnaryHandler {
	return func(ctx context.Context, req interface{}) (interface{}, error) {
		return expectedResp, nil
	}
}

// Helper function to create a basic erroring handler
func erroringHandler(expectedErr error) grpc.UnaryHandler {
	return func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, expectedErr
	}
}

func TestLoggingInterceptor_SuccessfulRPC_At100Percent(t *testing.T) {
	// Ensure 100% logging and a fresh, non-deterministic random source
	InitializeForTest(100, rand.NewSource(time.Now().UnixNano()))
	logBuffer.Reset() // Clear buffer before this test

	ctx := context.Background()
	info := &grpc.UnaryServerInfo{FullMethod: "/test.service/SuccessfulRPC"}
	req := &pb.GetObservedVariableRequest{Entity: "e1", Variable: "v1"}
	respProto := &pb.GetObservedVariableResponse{Places: []string{"p1"}}

	resp, err := LoggingInterceptor(ctx, req, info, successfulHandler(respProto))

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if !proto.Equal(resp.(proto.Message), respProto) {
		t.Fatalf("Expected response %v, got %v", respProto, resp)
	}

	loggedOutput := logBuffer.String()
	reqJSON, _ := json.Marshal(req)
	expectedLogSubString := "Request payload for method " + info.FullMethod + ": " + string(reqJSON)
	if !strings.Contains(loggedOutput, expectedLogSubString) {
		t.Errorf("Expected log to contain '%s', got '%s'", expectedLogSubString, loggedOutput)
	}
}

func TestLoggingInterceptor_FailedRPC_At100Percent(t *testing.T) {
	InitializeForTest(100, rand.NewSource(time.Now().UnixNano())) // Ensure 100% to check no logs on error
	logBuffer.Reset()

	ctx := context.Background()
	info := &grpc.UnaryServerInfo{FullMethod: "/test.service/FailedRPC"}
	req := &pb.GetObservedVariableRequest{Entity: "e2", Variable: "v2"}
	expectedErr := errors.New("handler failure")

	_, err := LoggingInterceptor(ctx, req, info, erroringHandler(expectedErr))

	if err == nil {
		t.Fatal("Expected an error, got nil")
	}
	if !errors.Is(err, expectedErr) {
		t.Errorf("Expected error %v, got %v", expectedErr, err)
	}

	loggedOutput := logBuffer.String()
	if strings.Contains(loggedOutput, "Request payload for method") {
		t.Errorf("Expected no request payload log for failed RPC, got '%s'", loggedOutput)
	}
}

func TestLoggingInterceptor_JSONMarshalFailure_At100Percent(t *testing.T) {
	InitializeForTest(100, rand.NewSource(time.Now().UnixNano())) // Ensure 100% for this test
	logBuffer.Reset()

	ctx := context.Background()
	info := &grpc.UnaryServerInfo{FullMethod: "/test.service/MarshalFailure"}
	req := struct{ BadField chan int }{BadField: make(chan int)} // Channels cause json.Marshal to fail
	expectedResp := "dummy_response_marshal_fail"
	
	handler := func(ctx context.Context, r interface{}) (interface{}, error) {
		return expectedResp, nil
	}

	resp, err := LoggingInterceptor(ctx, req, info, handler)

	if err != nil {
		t.Fatalf("Expected no error from interceptor itself, got %v", err)
	}
	if resp != expectedResp {
		t.Fatalf("Expected response %v, got %v", expectedResp, resp)
	}

	loggedOutput := logBuffer.String()
	if !strings.Contains(loggedOutput, "Failed to marshal request payload to JSON") {
		t.Errorf("Expected log to contain marshal failure message, got '%s'", loggedOutput)
	}
	// Crucially, the successful payload log should not appear
	if strings.Contains(loggedOutput, "Request payload for method "+info.FullMethod) {
		t.Errorf("Expected no successful request payload log due to marshal error, got '%s'", loggedOutput)
	}
}

func TestLoggingInterceptor_PercentageZero(t *testing.T) {
	InitializeForTest(0, rand.NewSource(time.Now().UnixNano())) // 0% logging
	logBuffer.Reset()

	ctx := context.Background()
	info := &grpc.UnaryServerInfo{FullMethod: "/test.service/ZeroPercent"}
	req := &pb.GetObservedVariableRequest{Entity: "e3", Variable: "v3"}
	respProto := &pb.GetObservedVariableResponse{Places: []string{"p3"}}

	for i := 0; i < 5; i++ { // Call multiple times to increase confidence
		_, err := LoggingInterceptor(ctx, req, info, successfulHandler(respProto))
		if err != nil {
			t.Fatalf("Iteration %d: Expected no error, got %v", i, err)
		}
	}

	loggedOutput := logBuffer.String()
	if strings.Contains(loggedOutput, "Request payload for method") {
		t.Errorf("Expected no request payload logs when percentage is 0%%, got '%s'", loggedOutput)
	}
}

func TestLoggingInterceptor_PercentageFiftyDeterministic(t *testing.T) {
	ctx := context.Background()
	info := &grpc.UnaryServerInfo{FullMethod: "/test.service/FiftyPercent"}
	req := &pb.GetObservedVariableRequest{Entity: "e4", Variable: "v4"}
	respProto := &pb.GetObservedVariableResponse{Places: []string{"p4"}}
	handler := successfulHandler(respProto)

	// Test case where RNG value (49) < loggingPercentage (50) -> should log
	t.Run("ShouldLogWhenRandomLessThanPercentage", func(t *testing.T) {
		InitializeForTest(50, &mockRandSource{Value: 49}) // internalRand.Intn(100) will be 49
		logBuffer.Reset()
		
		_, err := LoggingInterceptor(ctx, req, info, handler)
		if err != nil {t.Fatalf("Unexpected error: %v", err)}

		loggedOutput := logBuffer.String()
		reqJSON, _ := json.Marshal(req)
		expectedLog := "Request payload for method " + info.FullMethod + ": " + string(reqJSON)
		if !strings.Contains(loggedOutput, expectedLog) {
			t.Errorf("Expected log (RNG val 49 for 50%% threshold), got '%s'", loggedOutput)
		}
	})

	// Test case where RNG value (50) == loggingPercentage (50) -> should NOT log (since it's < percentage)
	t.Run("ShouldNotLogWhenRandomEqualToPercentage", func(t *testing.T) {
		InitializeForTest(50, &mockRandSource{Value: 50}) // internalRand.Intn(100) will be 50
		logBuffer.Reset()

		_, err := LoggingInterceptor(ctx, req, info, handler)
		if err != nil {t.Fatalf("Unexpected error: %v", err)}

		loggedOutput := logBuffer.String()
		if strings.Contains(loggedOutput, "Request payload for method") {
			t.Errorf("Expected no log (RNG val 50 for 50%% threshold), got '%s'", loggedOutput)
		}
	})
	
	// Test case where RNG value (75) > loggingPercentage (50) -> should NOT log
	t.Run("ShouldNotLogWhenRandomGreaterThanPercentage", func(t *testing.T) {
		InitializeForTest(50, &mockRandSource{Value: 75}) // internalRand.Intn(100) will be 75
		logBuffer.Reset()

		_, err := LoggingInterceptor(ctx, req, info, handler)
		if err != nil {t.Fatalf("Unexpected error: %v", err)}

		loggedOutput := logBuffer.String()
		if strings.Contains(loggedOutput, "Request payload for method") {
			t.Errorf("Expected no log (RNG val 75 for 50%% threshold), got '%s'", loggedOutput)
		}
	})

	// Test with a sequence of random numbers
	t.Run("SequenceOfRandomNumbers", func(t *testing.T) {
		// Sequence for Intn(100) will be: 0 (log), 99 (no log), 49 (log), 50 (no log)
		InitializeForTest(50, newMockRandSourceAdvanced([]int64{0, 99, 49, 50}))
		
		// Call 1 (RNG results in 0)
		logBuffer.Reset() 
		LoggingInterceptor(ctx, req, info, handler)
		if !strings.Contains(logBuffer.String(), "Request payload for method") {
			t.Error("Expected log for RNG resulting in 0")
		}

		// Call 2 (RNG results in 99)
		logBuffer.Reset() 
		LoggingInterceptor(ctx, req, info, handler)
		if strings.Contains(logBuffer.String(), "Request payload for method") {
			t.Error("Expected no log for RNG resulting in 99")
		}

		// Call 3 (RNG results in 49)
		logBuffer.Reset() 
		LoggingInterceptor(ctx, req, info, handler)
		if !strings.Contains(logBuffer.String(), "Request payload for method") {
			t.Error("Expected log for RNG resulting in 49")
		}

		// Call 4 (RNG results in 50)
		logBuffer.Reset() 
		LoggingInterceptor(ctx, req, info, handler)
		if strings.Contains(logBuffer.String(), "Request payload for method") {
			t.Error("Expected no log for RNG resulting in 50")
		}
	})

	// Cleanup after this specific test function using t.Cleanup to scope it
	t.Cleanup(func() {
		InitializeForTest(100, rand.NewSource(time.Now().UnixNano()))
	})
}
