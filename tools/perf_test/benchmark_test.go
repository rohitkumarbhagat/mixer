// Copyright 2026 Google LLC
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

package main

import (
	"context"
	"errors"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/datacommonsorg/mixer/internal/server/spanner"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/metadata"
)

type fakeCall struct {
	method               string
	variables            []string
	entities             []string
	date                 string
	containedInPlace     *v2.ContainedInPlace
	nodes                []string
	constrainedPlaces    []string
	constrainedImport    string
	numEntitiesExistence int
	includeDefinitions   bool
	logSQL               bool
}

type fakeQueryClient struct {
	calls          []fakeCall
	failAt         int
	observationSeq [][]*spanner.Observation
	existence      [][]string
	groupNodes     []*spanner.StatVarGroupNode
	filteredGroups map[string]*spanner.FilteredStatVarGroupNode
	topics         map[string]int
}

func (f *fakeQueryClient) addCall(ctx context.Context, call fakeCall) error {
	md, _ := metadata.FromIncomingContext(ctx)
	call.logSQL = len(md.Get(util.XLogSQL)) > 0
	f.calls = append(f.calls, call)
	if f.failAt == len(f.calls) {
		return errors.New("query failed")
	}
	return nil
}

func (f *fakeQueryClient) observations() []*spanner.Observation {
	if len(f.observationSeq) == 0 {
		return []*spanner.Observation{{VariableMeasured: "Count_Person", ObservationAbout: "country/USA"}}
	}
	index := len(f.calls) - 1
	if index >= len(f.observationSeq) {
		index = len(f.observationSeq) - 1
	}
	return f.observationSeq[index]
}

func (f *fakeQueryClient) GetObservations(ctx context.Context, variables []string, entities []string, date string) ([]*spanner.Observation, error) {
	if err := f.addCall(ctx, fakeCall{method: "GetObservations", variables: slices.Clone(variables), entities: slices.Clone(entities), date: date}); err != nil {
		return nil, err
	}
	return f.observations(), nil
}

func (f *fakeQueryClient) CheckVariableExistence(ctx context.Context, variables []string, entities []string) ([][]string, error) {
	if err := f.addCall(ctx, fakeCall{method: "CheckVariableExistence", variables: slices.Clone(variables), entities: slices.Clone(entities)}); err != nil {
		return nil, err
	}
	return f.existence, nil
}

func (f *fakeQueryClient) GetObservationsContainedInPlace(ctx context.Context, variables []string, containedInPlace *v2.ContainedInPlace, date string) ([]*spanner.Observation, error) {
	placeCopy := *containedInPlace
	if err := f.addCall(ctx, fakeCall{method: "GetObservationsContainedInPlace", variables: slices.Clone(variables), containedInPlace: &placeCopy, date: date}); err != nil {
		return nil, err
	}
	return f.observations(), nil
}

func (f *fakeQueryClient) GetStatVarGroupNode(ctx context.Context, nodes []string, includeDefinitions bool) ([]*spanner.StatVarGroupNode, error) {
	if err := f.addCall(ctx, fakeCall{method: "GetStatVarGroupNode", nodes: slices.Clone(nodes), includeDefinitions: includeDefinitions}); err != nil {
		return nil, err
	}
	return f.groupNodes, nil
}

func (f *fakeQueryClient) GetFilteredStatVarGroupNode(ctx context.Context, nodes []string, constrainedPlaces []string, constrainedImport string, numEntitiesExistence int, includeDefinitions bool) (map[string]*spanner.FilteredStatVarGroupNode, error) {
	if err := f.addCall(ctx, fakeCall{method: "GetFilteredStatVarGroupNode", nodes: slices.Clone(nodes), constrainedPlaces: slices.Clone(constrainedPlaces), constrainedImport: constrainedImport, numEntitiesExistence: numEntitiesExistence, includeDefinitions: includeDefinitions}); err != nil {
		return nil, err
	}
	return f.filteredGroups, nil
}

func (f *fakeQueryClient) GetFilteredTopic(ctx context.Context, nodes []string, constrainedPlaces []string, constrainedImport string, numEntitiesExistence int) (map[string]int, error) {
	if err := f.addCall(ctx, fakeCall{method: "GetFilteredTopic", nodes: slices.Clone(nodes), constrainedPlaces: slices.Clone(constrainedPlaces), constrainedImport: constrainedImport, numEntitiesExistence: numEntitiesExistence}); err != nil {
		return nil, err
	}
	return f.topics, nil
}

func TestExecuteQueryDispatchesAllMethods(t *testing.T) {
	date := "2025"
	ancestor := "country/USA"
	childType := "State"
	constrainedImport := "dc/s/Source"
	numEntities := 2
	includeDefinitions := true

	for _, tc := range []struct {
		method string
		inputs queryInputs
		want   fakeCall
	}{
		{method: "GetObservations", inputs: queryInputs{Variables: []string{"sv"}, Entities: []string{"place"}, Date: &date}, want: fakeCall{method: "GetObservations", variables: []string{"sv"}, entities: []string{"place"}, date: date}},
		{method: "CheckVariableExistence", inputs: queryInputs{Variables: []string{"sv"}, Entities: []string{"place"}}, want: fakeCall{method: "CheckVariableExistence", variables: []string{"sv"}, entities: []string{"place"}}},
		{method: "GetObservationsContainedInPlace", inputs: queryInputs{Variables: []string{"sv"}, Date: &date, Ancestor: &ancestor, ChildType: &childType}, want: fakeCall{method: "GetObservationsContainedInPlace", variables: []string{"sv"}, date: date, containedInPlace: &v2.ContainedInPlace{Ancestor: ancestor, ChildPlaceType: childType}}},
		{method: "GetStatVarGroupNode", inputs: queryInputs{Nodes: []string{"svg"}, IncludeDefinitions: &includeDefinitions}, want: fakeCall{method: "GetStatVarGroupNode", nodes: []string{"svg"}, includeDefinitions: true}},
		{method: "GetFilteredStatVarGroupNode", inputs: queryInputs{Nodes: []string{"svg"}, ConstrainedPlaces: []string{"place"}, ConstrainedImport: &constrainedImport, NumEntitiesExistence: &numEntities, IncludeDefinitions: &includeDefinitions}, want: fakeCall{method: "GetFilteredStatVarGroupNode", nodes: []string{"svg"}, constrainedPlaces: []string{"place"}, constrainedImport: constrainedImport, numEntitiesExistence: 2, includeDefinitions: true}},
		{method: "GetFilteredTopic", inputs: queryInputs{Nodes: []string{"topic"}, ConstrainedPlaces: []string{"place"}, ConstrainedImport: &constrainedImport, NumEntitiesExistence: &numEntities}, want: fakeCall{method: "GetFilteredTopic", nodes: []string{"topic"}, constrainedPlaces: []string{"place"}, constrainedImport: constrainedImport, numEntitiesExistence: 2}},
	} {
		t.Run(tc.method, func(t *testing.T) {
			client := &fakeQueryClient{}
			if _, err := executeQuery(context.Background(), client, querySpec{Method: tc.method, Inputs: tc.inputs}); err != nil {
				t.Fatalf("executeQuery() error = %v", err)
			}
			if len(client.calls) != 1 || !reflect.DeepEqual(client.calls[0], tc.want) {
				t.Fatalf("calls = %#v, want %#v", client.calls, tc.want)
			}
		})
	}
}

func TestRunBenchmarkLifecycleAndSQLLogging(t *testing.T) {
	client := &fakeQueryClient{}
	spec := observationSpec()
	result := caseReport{SamplesMS: []float64{}}
	profile := benchmarkProfile{Warmup: 2, Iterations: 3, Concurrency: 1}

	if err := runBenchmark(context.Background(), client, spec, true, &result, profile); err != nil {
		t.Fatalf("runBenchmark() error = %v", err)
	}
	if got, want := len(client.calls), 6; got != want {
		t.Fatalf("call count = %d, want %d", got, want)
	}
	for i, call := range client.calls {
		if call.logSQL != (i == 0) {
			t.Errorf("call %d logSQL = %v, want %v", i+1, call.logSQL, i == 0)
		}
	}
	if got, want := len(result.SamplesMS), 3; got != want {
		t.Fatalf("sample count = %d, want %d", got, want)
	}
	if result.PreflightResultDigest == "" || result.PreflightResultDigest != result.FinalResultDigest {
		t.Fatalf("result digests = %q and %q, want equal non-empty values", result.PreflightResultDigest, result.FinalResultDigest)
	}
	if result.Stats.Count != 3 {
		t.Fatalf("stats count = %d, want 3", result.Stats.Count)
	}
}

func TestRunBenchmarkReportsFailures(t *testing.T) {
	for _, tc := range []struct {
		name          string
		failAt        int
		wantPhase     string
		wantIteration int
		wantSamples   int
	}{
		{name: "preflight", failAt: 1, wantPhase: "preflight", wantIteration: 1},
		{name: "warmup", failAt: 3, wantPhase: "warmup", wantIteration: 2},
		{name: "measurement", failAt: 5, wantPhase: "measurement", wantIteration: 2, wantSamples: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &fakeQueryClient{failAt: tc.failAt}
			result := caseReport{SamplesMS: []float64{}}
			err := runBenchmark(context.Background(), client, observationSpec(), false, &result, benchmarkProfile{Warmup: 2, Iterations: 3, Concurrency: 1})
			if err == nil {
				t.Fatal("runBenchmark() error = nil, want error")
			}
			if result.Error == nil || result.Error.Phase != tc.wantPhase || result.Error.Iteration != tc.wantIteration {
				t.Fatalf("error report = %#v, want phase %q iteration %d", result.Error, tc.wantPhase, tc.wantIteration)
			}
			if len(result.SamplesMS) != tc.wantSamples {
				t.Fatalf("sample count = %d, want %d", len(result.SamplesMS), tc.wantSamples)
			}
		})
	}
}

func TestRunBenchmarkRejectsChangingResult(t *testing.T) {
	first := []*spanner.Observation{{VariableMeasured: "Count_Person", ObservationAbout: "country/USA"}}
	changed := []*spanner.Observation{{VariableMeasured: "Count_Household", ObservationAbout: "country/USA"}}
	client := &fakeQueryClient{observationSeq: [][]*spanner.Observation{first, first, changed}}
	result := caseReport{SamplesMS: []float64{}}

	err := runBenchmark(context.Background(), client, observationSpec(), false, &result, benchmarkProfile{Warmup: 0, Iterations: 2, Concurrency: 1})
	if err == nil {
		t.Fatal("runBenchmark() error = nil, want correctness error")
	}
	if result.Error == nil || result.Error.Phase != "correctness" {
		t.Fatalf("error report = %#v, want correctness phase", result.Error)
	}
	if result.PreflightResultDigest == result.FinalResultDigest {
		t.Fatal("preflight and final digests are equal, want mismatch")
	}
}

func observationSpec() querySpec {
	date := ""
	return querySpec{
		Name:   "observations",
		Method: "GetObservations",
		Inputs: queryInputs{Variables: []string{"Count_Person"}, Entities: []string{"country/USA"}, Date: &date},
	}
}

func TestNormalizeAndDigestDeterministicForEveryResultType(t *testing.T) {
	observationA := []*spanner.Observation{
		{VariableMeasured: "B", ObservationAbout: "place", FacetId: "2", Observations: spanner.TimeSeries{{Date: "2021", Value: "2"}, {Date: "2020", Value: "1"}}},
		{VariableMeasured: "A", ObservationAbout: "place", FacetId: "1"},
	}
	observationB := []*spanner.Observation{
		{VariableMeasured: "A", ObservationAbout: "place", FacetId: "1"},
		{VariableMeasured: "B", ObservationAbout: "place", FacetId: "2", Observations: spanner.TimeSeries{{Date: "2020", Value: "1"}, {Date: "2021", Value: "2"}}},
	}
	groupA := []*spanner.StatVarGroupNode{{SVG: "B", SubjectID: "2"}, {SVG: "A", SubjectID: "1"}}
	groupB := []*spanner.StatVarGroupNode{{SVG: "A", SubjectID: "1"}, {SVG: "B", SubjectID: "2"}}
	filteredA := map[string]*spanner.FilteredStatVarGroupNode{"node": {SVGChild: []*spanner.SVGChild{{SubjectID: "2"}, {SubjectID: "1"}}, ChildSV: []*spanner.ChildSV{{SubjectID: "2"}, {SubjectID: "1"}}, ChildSVG: []*spanner.ChildSVG{{SubjectID: "2"}, {SubjectID: "1"}}}}
	filteredB := map[string]*spanner.FilteredStatVarGroupNode{"node": {SVGChild: []*spanner.SVGChild{{SubjectID: "1"}, {SubjectID: "2"}}, ChildSV: []*spanner.ChildSV{{SubjectID: "1"}, {SubjectID: "2"}}, ChildSVG: []*spanner.ChildSVG{{SubjectID: "1"}, {SubjectID: "2"}}}}

	for _, tc := range []struct {
		name        string
		method      string
		left        any
		right       any
		wantSummary map[string]int64
	}{
		{name: "observations", method: "GetObservations", left: observationA, right: observationB, wantSummary: map[string]int64{"rows": 2, "observation_points": 2}},
		{name: "contained in", method: "GetObservationsContainedInPlace", left: observationA, right: observationB, wantSummary: map[string]int64{"rows": 2, "observation_points": 2}},
		{name: "existence", method: "CheckVariableExistence", left: [][]string{{"B", "2"}, {"A", "1"}}, right: [][]string{{"A", "1"}, {"B", "2"}}, wantSummary: map[string]int64{"pairs": 2}},
		{name: "stat var group", method: "GetStatVarGroupNode", left: groupA, right: groupB, wantSummary: map[string]int64{"rows": 2}},
		{name: "filtered stat var group", method: "GetFilteredStatVarGroupNode", left: filteredA, right: filteredB, wantSummary: map[string]int64{"nodes": 1, "svg_children": 2, "stat_var_children": 2, "child_svgs": 2}},
		{name: "filtered topic", method: "GetFilteredTopic", left: map[string]int{"B": 2, "A": 1}, right: map[string]int{"A": 1, "B": 2}, wantSummary: map[string]int64{"nodes": 2, "total_children": 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leftDigest, leftSummary, err := normalizeAndDigest(tc.method, tc.left)
			if err != nil {
				t.Fatalf("normalizeAndDigest(left) error = %v", err)
			}
			rightDigest, rightSummary, err := normalizeAndDigest(tc.method, tc.right)
			if err != nil {
				t.Fatalf("normalizeAndDigest(right) error = %v", err)
			}
			if leftDigest != rightDigest {
				t.Fatalf("digests differ: %s != %s", leftDigest, rightDigest)
			}
			if !reflect.DeepEqual(leftSummary, rightSummary) {
				t.Fatalf("summaries differ: %v != %v", leftSummary, rightSummary)
			}
			if !reflect.DeepEqual(leftSummary, tc.wantSummary) {
				t.Fatalf("summary = %v, want %v", leftSummary, tc.wantSummary)
			}
		})
	}
}

func TestInputDigestStableAndOrderSensitive(t *testing.T) {
	opts := cliOptions{Schema: "legacy", Warmup: 5, Iterations: 30}
	spec := observationSpec()
	spec.Inputs.Variables = []string{"Count_Person", "Count_Household"}
	first, err := newBenchmarkReport(opts, []byte("config"), spec)
	if err != nil {
		t.Fatalf("newBenchmarkReport() error = %v", err)
	}
	second, err := newBenchmarkReport(opts, []byte("config"), spec)
	if err != nil {
		t.Fatalf("newBenchmarkReport() error = %v", err)
	}
	if first.Cases[0].InputDigest != second.Cases[0].InputDigest {
		t.Fatal("identical inputs produced different digests")
	}

	reversed := observationSpec()
	reversed.Inputs.Variables = []string{"Count_Household", "Count_Person"}
	third, err := newBenchmarkReport(opts, []byte("config"), reversed)
	if err != nil {
		t.Fatalf("newBenchmarkReport() error = %v", err)
	}
	if first.Cases[0].InputDigest == third.Cases[0].InputDigest {
		t.Fatal("different ordered inputs produced the same digest")
	}
}

func TestReportJSONRoundTrip(t *testing.T) {
	opts := cliOptions{Schema: "multi_entity", Warmup: 0, Iterations: 1, EnvironmentLabel: "staging"}
	report, err := newBenchmarkReport(opts, []byte("secret config"), observationSpec())
	if err != nil {
		t.Fatalf("newBenchmarkReport() error = %v", err)
	}
	report.Cases[0].SamplesMS = []float64{12.5}
	report.Cases[0].Stats = calculateStats(report.Cases[0].SamplesMS)
	report.Cases[0].PreflightResultDigest = "result"
	report.Cases[0].FinalResultDigest = "result"

	path := t.TempDir() + "/report.json"
	if err := writeReport(path, report); err != nil {
		t.Fatalf("writeReport() error = %v", err)
	}
	fileInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("os.Stat() error = %v", err)
	}
	if got, want := fileInfo.Mode().Perm(), os.FileMode(0644); got != want {
		t.Fatalf("report permissions = %o, want %o", got, want)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("os.ReadFile() error = %v", err)
	}
	if strings.Contains(string(contents), "secret config") {
		t.Fatal("report contains raw config contents")
	}
	got, err := readReport(path)
	if err != nil {
		t.Fatalf("readReport() error = %v", err)
	}
	if !reflect.DeepEqual(got, report) {
		t.Fatalf("round-trip report differs:\n got %#v\nwant %#v", got, report)
	}
}

func TestCalculateStats(t *testing.T) {
	got := calculateStats([]float64{5, 1, 4, 2, 3, 100, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19})
	if got.Count != 20 || got.MinMS != 1 || got.MaxMS != 100 {
		t.Fatalf("count/min/max = %d/%.1f/%.1f", got.Count, got.MinMS, got.MaxMS)
	}
	if got.MeanMS != 14.5 || got.MedianMS != 10.5 {
		t.Fatalf("mean/median = %.1f/%.1f, want 14.5/10.5", got.MeanMS, got.MedianMS)
	}
	if got.P90MS != 18 || got.P95MS != 19 {
		t.Fatalf("p90/p95 = %.1f/%.1f, want 18/19", got.P90MS, got.P95MS)
	}
	if odd := calculateStats([]float64{3, 1, 2}); odd.MedianMS != 2 {
		t.Fatalf("odd median = %.1f, want 2", odd.MedianMS)
	}
}
