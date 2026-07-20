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
	"encoding/json"
	"fmt"
	"io"
	"os"
)

type classification string

const (
	classificationPass        classification = "PASS"
	classificationImprovement classification = "IMPROVEMENT"
	classificationRegression  classification = "REGRESSION"
	classificationInvalid     classification = "INVALID"
)

type comparison struct {
	Classification   classification
	Reason           string
	Baseline         caseReport
	Candidate        caseReport
	AbsoluteDeltaMS  float64
	RelativeDeltaPct float64
}

func compareCommand(opts cliOptions, stdout io.Writer, stderr io.Writer) int {
	baseline, err := readReport(opts.Baseline)
	if err != nil {
		fmt.Fprintf(stderr, "Failed to read baseline report: %v\n", err)
		return exitError
	}
	candidate, err := readReport(opts.Candidate)
	if err != nil {
		fmt.Fprintf(stderr, "Failed to read candidate report: %v\n", err)
		return exitError
	}

	result := compareReports(baseline, candidate, opts.RelativeThreshold, opts.AbsoluteThresholdMS)
	printComparison(stdout, result)
	return comparisonExitCode(result.Classification)
}

func readReport(path string) (benchmarkReport, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return benchmarkReport{}, err
	}
	var report benchmarkReport
	if err := json.Unmarshal(contents, &report); err != nil {
		return benchmarkReport{}, err
	}
	return report, nil
}

func compareReports(baseline benchmarkReport, candidate benchmarkReport, relativeThreshold float64, absoluteThresholdMS float64) comparison {
	result := comparison{Classification: classificationInvalid, Reason: "reports are incompatible"}
	if reason := incompatibilityReason(baseline, candidate); reason != "" {
		result.Reason = reason
		return result
	}

	result.Baseline = baseline.Cases[0]
	result.Candidate = candidate.Cases[0]
	result.AbsoluteDeltaMS = result.Candidate.Stats.MedianMS - result.Baseline.Stats.MedianMS
	result.RelativeDeltaPct = result.AbsoluteDeltaMS / result.Baseline.Stats.MedianMS * 100

	switch {
	case result.RelativeDeltaPct >= relativeThreshold && result.AbsoluteDeltaMS >= absoluteThresholdMS:
		result.Classification = classificationRegression
	case result.RelativeDeltaPct <= -relativeThreshold && result.AbsoluteDeltaMS <= -absoluteThresholdMS:
		result.Classification = classificationImprovement
	default:
		result.Classification = classificationPass
	}
	result.Reason = ""
	return result
}

func incompatibilityReason(baseline benchmarkReport, candidate benchmarkReport) string {
	if baseline.SchemaVersion != reportSchemaVersion || candidate.SchemaVersion != reportSchemaVersion {
		return fmt.Sprintf("unsupported report schema version: baseline=%d candidate=%d", baseline.SchemaVersion, candidate.SchemaVersion)
	}
	if len(baseline.Cases) != 1 || len(candidate.Cases) != 1 {
		return "each report must contain exactly one case"
	}
	if baseline.ConfigDigest == "" || baseline.ConfigDigest != candidate.ConfigDigest {
		return "config digests differ"
	}
	if baseline.EnvironmentLabel != candidate.EnvironmentLabel {
		return "environment labels differ"
	}
	if (baseline.Schema != "legacy" && baseline.Schema != "multi_entity") || baseline.Schema != candidate.Schema {
		return "schemas differ"
	}
	if baseline.Profile != candidate.Profile {
		return "benchmark profiles differ"
	}
	if baseline.Profile.Iterations <= 0 || baseline.Profile.Concurrency != 1 || baseline.Profile.Warmup < 0 {
		return "benchmark profile is invalid"
	}

	baseCase := baseline.Cases[0]
	candidateCase := candidate.Cases[0]
	if baseCase.InputDigest == "" || baseCase.InputDigest != candidateCase.InputDigest {
		return "input digests differ"
	}
	if baseCase.Error != nil || candidateCase.Error != nil {
		return "a report contains a query error"
	}
	if len(baseCase.SamplesMS) != baseline.Profile.Iterations || len(candidateCase.SamplesMS) != candidate.Profile.Iterations ||
		baseCase.Stats.Count != baseline.Profile.Iterations || candidateCase.Stats.Count != candidate.Profile.Iterations {
		return "a report has an incomplete sample count"
	}
	if baseCase.PreflightResultDigest == "" || baseCase.PreflightResultDigest != baseCase.FinalResultDigest {
		return "baseline preflight and final result digests differ"
	}
	if candidateCase.PreflightResultDigest == "" || candidateCase.PreflightResultDigest != candidateCase.FinalResultDigest {
		return "candidate preflight and final result digests differ"
	}
	if baseCase.FinalResultDigest != candidateCase.FinalResultDigest {
		return "baseline and candidate result digests differ"
	}
	if baseCase.Stats.MedianMS <= 0 {
		return "baseline median must be positive"
	}
	return ""
}

func comparisonExitCode(value classification) int {
	switch value {
	case classificationPass, classificationImprovement:
		return exitSuccess
	case classificationRegression:
		return exitRegression
	case classificationInvalid:
		return exitInvalid
	default:
		return exitError
	}
}

func printComparison(output io.Writer, result comparison) {
	if result.Classification == classificationInvalid {
		fmt.Fprintf(output, "classification=%s\nreason=%s\n", result.Classification, result.Reason)
		return
	}
	fmt.Fprintf(output, "baseline median=%.3f ms p90=%.3f ms p95=%.3f ms summary=%s\n",
		result.Baseline.Stats.MedianMS, result.Baseline.Stats.P90MS, result.Baseline.Stats.P95MS, formatSummary(result.Baseline.ResultSummary))
	fmt.Fprintf(output, "candidate median=%.3f ms p90=%.3f ms p95=%.3f ms summary=%s\n",
		result.Candidate.Stats.MedianMS, result.Candidate.Stats.P90MS, result.Candidate.Stats.P95MS, formatSummary(result.Candidate.ResultSummary))
	fmt.Fprintf(output, "delta=%+.3f ms (%+.2f%%)\n", result.AbsoluteDeltaMS, result.RelativeDeltaPct)
	fmt.Fprintf(output, "classification=%s\n", result.Classification)
}
