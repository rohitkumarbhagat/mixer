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
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
)

var (
	// internalRand is a package-level random number generator.
	// It's seeded once in init() and can be replaced for testing.
	internalRand *rand.Rand
	// currentLoggingPercentage stores the active logging percentage.
	// It's protected by percentageMux.
	currentLoggingPercentage int
	percentageMux          sync.RWMutex
	randOnce               sync.Once // To initialize internalRand only once
)

// ensureRandInitialized initializes internalRand if it hasn't been already.
func ensureRandInitialized() {
	randOnce.Do(func() {
		if internalRand == nil {
			internalRand = rand.New(rand.NewSource(time.Now().UnixNano()))
		}
	})
}

// SetLoggingPercentage updates the percentage of requests that will be logged.
// It clamps the input percentage between 0 and 100.
// It also ensures that the random number generator is initialized.
func SetLoggingPercentage(percentage int) {
	percentageMux.Lock()
	defer percentageMux.Unlock()

	ensureRandInitialized() // Ensure RNG is ready

	if percentage < 0 {
		currentLoggingPercentage = 0
	} else if percentage > 100 {
		currentLoggingPercentage = 100
	} else {
		currentLoggingPercentage = percentage
	}
	// Set default if it hasn't been set by this function yet (e.g. if LoggingInterceptor is called first)
	// This case is less likely now that main.go will call SetLoggingPercentage.
	// However, keeping it for robustness if tests or other paths don't call SetLoggingPercentage.
	// Actually, the default should be handled by the flag in main.go.
	// Here, we just log what it's being set to.
	log.Printf("Logging percentage set to: %d%%", currentLoggingPercentage)
}

// InitializeForTest allows tests to set a specific rand.Source and logging percentage.
// This is a more comprehensive test setup function.
func InitializeForTest(percentage int, source rand.Source) {
	percentageMux.Lock()
	defer percentageMux.Unlock()

	internalRand = rand.New(source) // Set specific RNG for test
	if percentage < 0 {
		currentLoggingPercentage = 0
	} else if percentage > 100 {
		currentLoggingPercentage = 100
	} else {
		currentLoggingPercentage = percentage
	}
	log.Printf("Logging interceptor initialized FOR TEST with percentage: %d%%", currentLoggingPercentage)
}

// SetRandSourceForTest is a legacy test helper, prefer InitializeForTest.
// It allows tests to set a specific rand.Source for deterministic testing.
// This function should only be called by tests.
func SetRandSourceForTest(source rand.Source) {
	// This function is typically called before any interceptor logic runs in tests.
	// It should be used in conjunction with setting the percentage if needed.
	ensureRandInitialized() // Ensure RNG is ready if not already set by InitializeForTest
	internalRand = rand.New(source)
}

// LoggingInterceptor is a gRPC unary server interceptor that logs the request payload
// based on a configured percentage.
func LoggingInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	ensureRandInitialized() // Ensure RNG is initialized, especially if SetLoggingPercentage wasn't called.

	resp, err := handler(ctx, req)
	if err != nil {
		return resp, err
	}

	// We only care about successful non-streaming RPCs.
	// TODO(LS): Add check for streaming RPCs if necessary. For now, this is a unary interceptor.

	percentageMux.RLock()
	// Default to 100% if not explicitly set. This is a safeguard,
	// as main.go should set it.
	activePercentage := 100
	if currentLoggingPercentage >= 0 && currentLoggingPercentage <= 100 { // Check if it was set to a valid range
		// This check is a bit redundant if SetLoggingPercentage always clamps.
		// More accurately, currentLoggingPercentage should have its zero value (0) if not set,
		// but the flag in main.go will default to 100.
		// Let's assume currentLoggingPercentage is initialized to 100 by flag default if not set otherwise.
		// For safety, if it was somehow not set by main or tests, what should it be?
		// The flag in main.go will have a default of 100. So this path should ideally not be hit in prod.
		// For testing, InitializeForTest or SetLoggingPercentage will set it.
		// If currentLoggingPercentage is 0 (default int value) and it wasn't set by main.go,
		// it implies an issue. Let's use the value directly.
		// If SetLoggingPercentage has not been called, currentLoggingPercentage is 0.
		// The flag in main.go defaults to 100. SetLoggingPercentage will be called by main.
		// So, currentLoggingPercentage will hold the flag value.
		activePercentage = currentLoggingPercentage
	}
	percentageMux.RUnlock()

	// Log only if a random number is less than the active percentage.
	if internalRand.Intn(100) < activePercentage {
		payload, err := json.Marshal(req)
		if err != nil {
			log.Printf("Failed to marshal request payload to JSON: %v", err)
			// Still return success, as logging failure should not break the RPC.
			return resp, nil
		}
		log.Printf("Request payload for method %s: %s", info.FullMethod, string(payload))
	}
	return resp, nil
}
