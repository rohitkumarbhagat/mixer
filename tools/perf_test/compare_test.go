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

import "testing"

func TestCompareReportsClassifications(t *testing.T) {
	for _, tc := range []struct {
		name            string
		baselineMedian  float64
		candidateMedian float64
		want            classification
	}{
		{name: "matching is pass", baselineMedian: 100, candidateMedian: 100, want: classificationPass},
		{name: "material improvement", baselineMedian: 200, candidateMedian: 180, want: classificationImprovement},
		{name: "relative only slowdown", baselineMedian: 100, candidateMedian: 115, want: classificationPass},
		{name: "absolute only slowdown", baselineMedian: 1000, candidateMedian: 1025, want: classificationPass},
		{name: "both threshold slowdown", baselineMedian: 200, candidateMedian: 220, want: classificationRegression},
	} {
		t.Run(tc.name, func(t *testing.T) {
			baseline := compatibleReport(tc.baselineMedian)
			candidate := compatibleReport(tc.candidateMedian)
			got := compareReports(baseline, candidate, 10, 20)
			if got.Classification != tc.want {
				t.Fatalf("classification = %s, want %s (reason %q)", got.Classification, tc.want, got.Reason)
			}
		})
	}
}

func TestCompareReportsRejectsIncompatibilities(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*benchmarkReport, *benchmarkReport)
	}{
		{name: "schema version", change: func(_, candidate *benchmarkReport) { candidate.SchemaVersion = 2 }},
		{name: "config", change: func(_, candidate *benchmarkReport) { candidate.ConfigDigest = "other" }},
		{name: "environment", change: func(_, candidate *benchmarkReport) { candidate.EnvironmentLabel = "other" }},
		{name: "schema", change: func(_, candidate *benchmarkReport) { candidate.Schema = "multi_entity" }},
		{name: "unsupported schema", change: func(baseline, candidate *benchmarkReport) { baseline.Schema = "other"; candidate.Schema = "other" }},
		{name: "profile", change: func(_, candidate *benchmarkReport) { candidate.Profile.Warmup++ }},
		{name: "input", change: func(_, candidate *benchmarkReport) { candidate.Cases[0].InputDigest = "other" }},
		{name: "result", change: func(_, candidate *benchmarkReport) {
			candidate.Cases[0].PreflightResultDigest = "other"
			candidate.Cases[0].FinalResultDigest = "other"
		}},
		{name: "baseline correctness", change: func(baseline, _ *benchmarkReport) { baseline.Cases[0].FinalResultDigest = "changed" }},
		{name: "candidate correctness", change: func(_, candidate *benchmarkReport) { candidate.Cases[0].FinalResultDigest = "changed" }},
		{name: "sample count", change: func(_, candidate *benchmarkReport) { candidate.Cases[0].SamplesMS = candidate.Cases[0].SamplesMS[:2] }},
		{name: "stats count", change: func(_, candidate *benchmarkReport) { candidate.Cases[0].Stats.Count = 2 }},
		{name: "query error", change: func(_, candidate *benchmarkReport) {
			candidate.Cases[0].Error = &benchmarkError{Phase: "measurement", Iteration: 1, Message: "failed"}
		}},
		{name: "case count", change: func(_, candidate *benchmarkReport) { candidate.Cases = append(candidate.Cases, candidate.Cases[0]) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			baseline := compatibleReport(100)
			candidate := compatibleReport(100)
			tc.change(&baseline, &candidate)
			got := compareReports(baseline, candidate, 10, 20)
			if got.Classification != classificationInvalid || got.Reason == "" {
				t.Fatalf("comparison = %#v, want INVALID with reason", got)
			}
		})
	}
}

func TestComparisonExitCodes(t *testing.T) {
	for value, want := range map[classification]int{
		classificationPass:        exitSuccess,
		classificationImprovement: exitSuccess,
		classificationRegression:  exitRegression,
		classificationInvalid:     exitInvalid,
	} {
		if got := comparisonExitCode(value); got != want {
			t.Errorf("comparisonExitCode(%s) = %d, want %d", value, got, want)
		}
	}
}

func compatibleReport(median float64) benchmarkReport {
	samples := []float64{median - 1, median, median + 1}
	return benchmarkReport{
		SchemaVersion:    reportSchemaVersion,
		EnvironmentLabel: "benchmark-db",
		ConfigDigest:     "config",
		Schema:           "legacy",
		Profile:          benchmarkProfile{Warmup: 5, Iterations: 3, Concurrency: 1},
		Cases: []caseReport{{
			Name:                  "case",
			Method:                "GetObservations",
			InputDigest:           "input",
			PreflightResultDigest: "result",
			FinalResultDigest:     "result",
			ResultSummary:         map[string]int64{"rows": 1},
			SamplesMS:             samples,
			Stats:                 calculateStats(samples),
		}},
	}
}
