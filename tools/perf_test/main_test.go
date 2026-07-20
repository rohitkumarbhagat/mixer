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
	"io"
	"slices"
	"strings"
	"testing"
)

func TestExecuteCLICommandErrorsExitOne(t *testing.T) {
	for _, args := range [][]string{
		{"-mode=unsupported"},
		{"-mode=compare", "-baseline=/path/that/does/not/exist", "-candidate=/also/missing"},
	} {
		if got := executeCLI(args, io.Discard, io.Discard); got != exitError {
			t.Errorf("executeCLI(%v) = %d, want %d", args, got, exitError)
		}
	}
}

func TestValidateOptions(t *testing.T) {
	validRun := cliOptions{
		Mode:       "run",
		Method:     "GetObservations",
		Variables:  "Count_Person",
		Entities:   "country/USA",
		Schema:     "legacy",
		Warmup:     5,
		Iterations: 30,
		Output:     "/tmp/report.json",
	}
	validCompare := cliOptions{
		Mode:                "compare",
		Baseline:            "/tmp/baseline.json",
		Candidate:           "/tmp/candidate.json",
		RelativeThreshold:   10,
		AbsoluteThresholdMS: 20,
	}

	for _, tc := range []struct {
		name    string
		opts    cliOptions
		wantErr string
	}{
		{name: "valid run", opts: validRun},
		{name: "valid compare ignores run flags", opts: validCompare},
		{name: "unsupported mode", opts: withOption(validRun, func(opts *cliOptions) { opts.Mode = "other" }), wantErr: "unsupported mode"},
		{name: "missing schema", opts: withOption(validRun, func(opts *cliOptions) { opts.Schema = "" }), wantErr: "schema"},
		{name: "unsupported schema", opts: withOption(validRun, func(opts *cliOptions) { opts.Schema = "new" }), wantErr: "schema"},
		{name: "negative warmup", opts: withOption(validRun, func(opts *cliOptions) { opts.Warmup = -1 }), wantErr: "warmup"},
		{name: "zero iterations", opts: withOption(validRun, func(opts *cliOptions) { opts.Iterations = 0 }), wantErr: "iterations"},
		{name: "missing output", opts: withOption(validRun, func(opts *cliOptions) { opts.Output = "" }), wantErr: "output"},
		{name: "missing baseline", opts: withOption(validCompare, func(opts *cliOptions) { opts.Baseline = "" }), wantErr: "baseline and candidate"},
		{name: "missing candidate", opts: withOption(validCompare, func(opts *cliOptions) { opts.Candidate = "" }), wantErr: "baseline and candidate"},
		{name: "negative relative threshold", opts: withOption(validCompare, func(opts *cliOptions) { opts.RelativeThreshold = -1 }), wantErr: "thresholds"},
		{name: "negative absolute threshold", opts: withOption(validCompare, func(opts *cliOptions) { opts.AbsoluteThresholdMS = -1 }), wantErr: "thresholds"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateOptions(tc.opts)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("validateOptions() error = %v", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("validateOptions() error = %v, want error containing %q", err, tc.wantErr)
			}
		})
	}
}

func withOption(opts cliOptions, update func(*cliOptions)) cliOptions {
	update(&opts)
	return opts
}

func TestValidateInputsAllMethods(t *testing.T) {
	for _, tc := range []struct {
		name                string
		method              string
		variables           string
		entities            string
		ancestor            string
		childType           string
		nodes               string
		constrainedEntities string
		wantErr             bool
	}{
		{name: "observations valid", method: "GetObservations", variables: "Count_Person", entities: "country/USA"},
		{name: "observations needs variables", method: "GetObservations", entities: "country/USA", wantErr: true},
		{name: "observations needs entities", method: "GetObservations", variables: "Count_Person", wantErr: true},
		{name: "existence valid", method: "CheckVariableExistence", variables: "Count_Person", entities: "country/USA"},
		{name: "existence needs variables", method: "CheckVariableExistence", entities: "country/USA", wantErr: true},
		{name: "existence needs entities", method: "CheckVariableExistence", variables: "Count_Person", wantErr: true},
		{name: "contained in valid", method: "GetObservationsContainedInPlace", variables: "Count_Person", ancestor: "country/USA", childType: "State"},
		{name: "contained in needs variables", method: "GetObservationsContainedInPlace", ancestor: "country/USA", childType: "State", wantErr: true},
		{name: "contained in needs ancestor", method: "GetObservationsContainedInPlace", variables: "Count_Person", childType: "State", wantErr: true},
		{name: "contained in needs child type", method: "GetObservationsContainedInPlace", variables: "Count_Person", ancestor: "country/USA", wantErr: true},
		{name: "stat var group valid", method: "GetStatVarGroupNode", nodes: "dc/g/Demographics"},
		{name: "stat var group needs nodes", method: "GetStatVarGroupNode", wantErr: true},
		{name: "filtered stat var group valid", method: "GetFilteredStatVarGroupNode", nodes: "dc/g/Demographics", constrainedEntities: "country/USA"},
		{name: "filtered stat var group needs constraints", method: "GetFilteredStatVarGroupNode", nodes: "dc/g/Demographics", wantErr: true},
		{name: "filtered topic valid", method: "GetFilteredTopic", nodes: "dc/topic/Demographics", constrainedEntities: "country/USA"},
		{name: "filtered topic needs nodes", method: "GetFilteredTopic", constrainedEntities: "country/USA", wantErr: true},
		{name: "unknown method", method: "Unknown", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateInputs(tc.method, tc.variables, tc.entities, tc.ancestor, tc.childType, tc.nodes, tc.constrainedEntities, 0)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateInputs() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestValidateInputsBulkVariableGroupInfoMethods(t *testing.T) {
	for _, tc := range []struct {
		name                string
		method              string
		nodes               string
		constrainedEntities string
		numEntities         int
		wantErr             bool
	}{
		{
			name:    "stat var group node accepts nodes",
			method:  "GetStatVarGroupNode",
			nodes:   "dc/g/Agriculture",
			wantErr: false,
		},
		{
			name:    "stat var group node rejects missing nodes",
			method:  "GetStatVarGroupNode",
			wantErr: true,
		},
		{
			name:                "filtered stat var group node accepts constraints",
			method:              "GetFilteredStatVarGroupNode",
			nodes:               "dc/g/Agriculture",
			constrainedEntities: "country/USA,country/IND",
			numEntities:         2,
			wantErr:             false,
		},
		{
			name:                "filtered topic accepts source constraint",
			method:              "GetFilteredTopic",
			nodes:               "dc/topic/Demographics",
			constrainedEntities: "dc/s/WorldBank",
			wantErr:             false,
		},
		{
			name:                "filtered topic rejects missing nodes",
			method:              "GetFilteredTopic",
			constrainedEntities: "country/USA",
			wantErr:             true,
		},
		{
			name:    "filtered topic rejects missing constraints",
			method:  "GetFilteredTopic",
			nodes:   "dc/topic/Demographics",
			wantErr: true,
		},
		{
			name:                "filtered topic rejects empty comma constraints",
			method:              "GetFilteredTopic",
			nodes:               "dc/topic/Demographics",
			constrainedEntities: " , ",
			wantErr:             true,
		},
		{
			name:                "filtered topic rejects negative entity threshold",
			method:              "GetFilteredTopic",
			nodes:               "dc/topic/Demographics",
			constrainedEntities: "country/USA",
			numEntities:         -1,
			wantErr:             true,
		},
		{
			name:                "filtered topic rejects multiple import constraints",
			method:              "GetFilteredTopic",
			nodes:               "dc/topic/Demographics",
			constrainedEntities: "dc/s/WorldBank,dc/d/SomeDataset",
			wantErr:             true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateInputs(tc.method, "", "", "", "", tc.nodes, tc.constrainedEntities, tc.numEntities)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateInputs() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestParseConstrainedEntities(t *testing.T) {
	places, constrainedImport, err := parseConstrainedEntities("country/USA, dc/s/WorldBank, country/IND")
	if err != nil {
		t.Fatalf("parseConstrainedEntities() returned error: %v", err)
	}
	if want := []string{"country/USA", "country/IND"}; !slices.Equal(places, want) {
		t.Fatalf("places = %v, want %v", places, want)
	}
	if want := "dc/s/WorldBank"; constrainedImport != want {
		t.Fatalf("constrainedImport = %q, want %q", constrainedImport, want)
	}
}

func TestParseConstrainedEntitiesRejectsMultipleImports(t *testing.T) {
	_, _, err := parseConstrainedEntities("dc/s/WorldBank,dc/d/Dataset")
	if err == nil {
		t.Fatal("parseConstrainedEntities() returned nil error, want error")
	}
}
