// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package emulator

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"github.com/datacommonsorg/mixer/internal/server/dispatcher"
	sdmxformat "github.com/datacommonsorg/mixer/internal/server/sdmx/format"
	service "github.com/datacommonsorg/mixer/internal/server/sdmx/service"
	mixerspanner "github.com/datacommonsorg/mixer/internal/server/spanner"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSDMXData(t *testing.T) {
	sdmxService := newSDMXService(requireSuite(t).spannerClient)
	tests := []struct {
		name   string
		query  string
		golden string
	}{
		{
			name:   "two entity shape",
			query:  "c[variableMeasured]=Count_Migration&c[sourceCountry]=country%2FUSA&c[destinationCountry]=country%2FCAN",
			golden: "data_two_entities.csv",
		},
		{
			name:   "fallback observationAbout",
			query:  "c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA",
			golden: "data_fallback_observation_about.csv",
		},
		{
			name:   "three entity shape",
			query:  "c[variableMeasured]=Count_MigrationByTransportMode&c[destinationCountry]=country%2FCAN&c[sourceCountry]=country%2FUSA&c[transportMode]=Air&c[unit]=Count&c[measurementMethod]=Census&c[observationPeriod]=P1Y&c[provenance]=dc%2Fbase%2FHumanReadableStatVars",
			golden: "data_three_entities.csv",
		},
		{
			name:   "explicit middle observationAbout",
			query:  "c[variableMeasured]=Count_MigrationByObservationAbout&c[destinationCountry]=country%2FCAN&c[observationAbout]=country%2FMEX&c[sourceCountry]=country%2FUSA",
			golden: "data_explicit_observation_about.csv",
		},
		{
			name:   "compatible stat variables",
			query:  "c[variableMeasured]=Count_Migration,Count_Refugee",
			golden: "data_compatible_stat_vars.csv",
		},
		{
			name: "multiple values for every dimension",
			query: "c[variableMeasured]=Count_Migration,Count_Refugee&" +
				"c[destinationCountry]=country%2FCAN,country%2FMEX&" +
				"c[sourceCountry]=country%2FUSA,country%2FIND&" +
				"c[unit]=Count,Percent&c[measurementMethod]=Census,Survey&" +
				"c[observationPeriod]=P1Y,P1M&" +
				"c[provenance]=dc%2Fbase%2FHumanReadableStatVars,dc%2Fbase%2FOther",
			golden: "data_multiple_values.csv",
		},
		{
			name:   "empty result",
			query:  "c[variableMeasured]=Count_Migration&c[destinationCountry]=country%2FZZZ",
			golden: "data_empty.csv",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			response, err := sdmxService.Data(context.Background(), emulatorDataRequest(testCase.query))
			if err != nil {
				t.Fatalf("Data() error = %v", err)
			}
			if response.ContentType != sdmxformat.CSVContentType {
				t.Fatalf("Data() content type = %q, want %q", response.ContentType, sdmxformat.CSVContentType)
			}
			compareEmulatorGolden(t, testCase.golden, string(response.Body), false)
		})
	}
}

func TestSDMXAvailability(t *testing.T) {
	sdmxService := newSDMXService(requireSuite(t).spannerClient)
	tests := []struct {
		name      string
		component string
		query     string
		golden    string
	}{
		{
			name:      "third entity component",
			component: "transportMode",
			query:     "c[variableMeasured]=Count_MigrationByTransportMode&c[destinationCountry]=country%2FCAN&c[sourceCountry]=country%2FUSA",
			golden:    "availability_transport_mode.json",
		},
		{
			name:      "explicit middle observationAbout",
			component: "observationAbout",
			query:     "c[variableMeasured]=Count_MigrationByObservationAbout&c[destinationCountry]=country%2FCAN&c[sourceCountry]=country%2FUSA",
			golden:    "availability_explicit_observation_about.json",
		},
		{
			name:      "fixed dimension",
			component: "unit",
			query: "c[variableMeasured]=Count_Migration,Count_Refugee&" +
				"c[destinationCountry]=country%2FCAN,country%2FMEX&" +
				"c[sourceCountry]=country%2FUSA,country%2FIND",
			golden: "availability_unit.json",
		},
		{
			name:      "fallback observationAbout",
			component: "observationAbout",
			query:     "c[variableMeasured]=Count_Person",
			golden:    "availability_fallback_observation_about.json",
		},
		{
			name:      "variable measured",
			component: "variableMeasured",
			query:     "c[variableMeasured]=Count_Migration,Count_Refugee",
			golden:    "availability_variable_measured.json",
		},
		{
			name:      "empty result",
			component: "sourceCountry",
			query:     "c[variableMeasured]=Count_Migration&c[destinationCountry]=country%2FZZZ",
			golden:    "availability_empty.json",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			response, err := sdmxService.Availability(context.Background(), emulatorAvailabilityRequest(testCase.component, testCase.query))
			if err != nil {
				t.Fatalf("Availability() error = %v", err)
			}
			if response.ContentType != sdmxformat.StructureJSONType {
				t.Fatalf("Availability() content type = %q, want %q", response.ContentType, sdmxformat.StructureJSONType)
			}
			compareEmulatorGolden(t, testCase.golden, string(response.Body), true)
		})
	}
}

func TestSDMXRejectsIncompatibleStatVariableShapes(t *testing.T) {
	sdmxService := newSDMXService(requireSuite(t).spannerClient)
	_, err := sdmxService.Data(context.Background(), emulatorDataRequest(
		"c[variableMeasured]=Count_Migration,Count_Person",
	))
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Data() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
	}
	if !strings.Contains(status.Convert(err).Message(), "incompatible observationProperties") {
		t.Fatalf("Data() message = %q, want incompatible observationProperties", status.Convert(err).Message())
	}
}

func newSDMXService(client mixerspanner.SpannerClient) *service.Service {
	spannerSource := mixerspanner.NewSpannerDataSource(client, nil)
	sources := datasources.NewDataSources([]datasource.DataSource{spannerSource}, nil)
	return service.New(dispatcher.NewDispatcher(nil, sources))
}

func emulatorDataRequest(query string) service.Request {
	tail := "dataflow/DC/DF_OBS/1.0.0/*"
	return service.Request{Tail: tail, OriginalURI: "/sdmx/v3/data/" + tail + "?" + query}
}

func emulatorAvailabilityRequest(component, query string) service.Request {
	tail := "dataflow/DC/DF_OBS/1.0.0/*/" + component
	return service.Request{Tail: tail, OriginalURI: "/sdmx/v3/availability/" + tail + "?" + query}
}

func compareEmulatorGolden(t *testing.T, filename, actual string, normalizeJSON bool) {
	t.Helper()
	directory := filepath.Join("testdata", "sdmx")
	if normalizeJSON {
		actual = normalizedJSON(t, actual)
	} else {
		actual = strings.ReplaceAll(actual, "\r\n", "\n")
	}
	if test.GenerateGolden {
		if err := test.WriteGolden(actual, directory, filename); err != nil {
			t.Fatalf("WriteGolden(%q) error = %v", filename, err)
		}
		return
	}
	expected, err := test.ReadGolden(directory, filename)
	if err != nil {
		t.Fatalf("ReadGolden(%q) error = %v", filename, err)
	}
	if normalizeJSON {
		expected = normalizedJSON(t, expected)
	} else {
		expected = strings.ReplaceAll(expected, "\r\n", "\n")
	}
	if diff := cmp.Diff(expected, actual); diff != "" {
		t.Errorf("response mismatch (-want +got):\n%s", diff)
	}
}

func normalizedJSON(t *testing.T, value string) string {
	t.Helper()
	var decoded interface{}
	if err := json.Unmarshal([]byte(value), &decoded); err != nil {
		t.Fatalf("invalid JSON response: %v", err)
	}
	formatted, err := json.MarshalIndent(decoded, "", "  ")
	if err != nil {
		t.Fatalf("format JSON response: %v", err)
	}
	return string(formatted)
}
