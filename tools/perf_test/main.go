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
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/datacommonsorg/mixer/internal/server/spanner"
)

const (
	datasetPrefix = "dc/d/"
	sourcePrefix  = "dc/s/"

	exitSuccess    = 0
	exitError      = 1
	exitRegression = 2
	exitInvalid    = 3
)

type cliOptions struct {
	Mode                 string
	Method               string
	Variables            string
	Entities             string
	Nodes                string
	ConstrainedEntities  string
	Ancestor             string
	ChildType            string
	Config               string
	Date                 string
	NumEntitiesExistence int
	IncludeDefinitions   bool
	Schema               string
	Warmup               int
	Iterations           int
	Output               string
	Name                 string
	EnvironmentLabel     string
	LogSQL               bool
	Baseline             string
	Candidate            string
	RelativeThreshold    float64
	AbsoluteThresholdMS  float64
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))
	os.Exit(executeCLI(os.Args[1:], os.Stdout, os.Stderr))
}

func executeCLI(args []string, stdout io.Writer, stderr io.Writer) int {
	opts, err := parseCLI(args)
	if err != nil {
		fmt.Fprintf(stderr, "Invalid command: %v\n", err)
		return exitError
	}

	switch opts.Mode {
	case "run":
		return runCommand(context.Background(), opts, stdout, stderr)
	case "compare":
		return compareCommand(opts, stdout, stderr)
	default:
		panic("validated mode was not handled")
	}
}

func parseCLI(args []string) (cliOptions, error) {
	var opts cliOptions
	fs := flag.NewFlagSet("perf_test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	fs.StringVar(&opts.Mode, "mode", "run", "Mode: run or compare")
	fs.StringVar(&opts.Method, "method", "GetObservations", "SpannerClient method to benchmark")
	fs.StringVar(&opts.Variables, "variables", "", "Comma-separated list of variables")
	fs.StringVar(&opts.Entities, "entities", "", "Comma-separated list of entities")
	fs.StringVar(&opts.Nodes, "nodes", "", "Comma-separated list of StatVarGroup or Topic nodes")
	fs.StringVar(&opts.ConstrainedEntities, "constrained_entities", "", "Comma-separated constrained entities")
	fs.StringVar(&opts.Ancestor, "ancestor", "", "Ancestor place for contained-in queries")
	fs.StringVar(&opts.ChildType, "child_type", "", "Child place type for contained-in queries")
	fs.StringVar(&opts.Config, "config", "deploy/storage/spanner_graph_info.yaml", "Path to Spanner graph info YAML")
	fs.StringVar(&opts.Date, "date", "", "Optional date filter")
	fs.IntVar(&opts.NumEntitiesExistence, "num_entities_existence", 0, "Minimum constrained entities with observations")
	fs.BoolVar(&opts.IncludeDefinitions, "include_definitions", false, "Include StatVarGroup definitions")
	fs.StringVar(&opts.Schema, "schema", "", "Schema: legacy or multi_entity")
	fs.IntVar(&opts.Warmup, "warmup", 5, "Number of untimed warm-up calls")
	fs.IntVar(&opts.Iterations, "iterations", 30, "Number of measured calls")
	fs.StringVar(&opts.Output, "output", "", "Output JSON report path")
	fs.StringVar(&opts.Name, "name", "", "Case name (defaults to method)")
	fs.StringVar(&opts.EnvironmentLabel, "environment_label", "", "Benchmark environment label")
	fs.BoolVar(&opts.LogSQL, "log_sql", false, "Log SQL for the preflight call only")
	fs.StringVar(&opts.Baseline, "baseline", "", "Baseline JSON report path")
	fs.StringVar(&opts.Candidate, "candidate", "", "Candidate JSON report path")
	fs.Float64Var(&opts.RelativeThreshold, "relative_threshold", 10, "Relative median threshold in percent")
	fs.Float64Var(&opts.AbsoluteThresholdMS, "absolute_threshold_ms", 20, "Absolute median threshold in milliseconds")

	if err := fs.Parse(args); err != nil {
		return cliOptions{}, err
	}
	if fs.NArg() != 0 {
		return cliOptions{}, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	if err := validateOptions(opts); err != nil {
		return cliOptions{}, err
	}
	return opts, nil
}

func validateOptions(opts cliOptions) error {
	switch opts.Mode {
	case "run":
		if opts.Schema != "legacy" && opts.Schema != "multi_entity" {
			return fmt.Errorf("schema must be explicitly set to legacy or multi_entity")
		}
		if opts.Warmup < 0 {
			return fmt.Errorf("warmup must be non-negative")
		}
		if opts.Iterations <= 0 {
			return fmt.Errorf("iterations must be positive")
		}
		if strings.TrimSpace(opts.Output) == "" {
			return fmt.Errorf("output is required in run mode")
		}
		return validateInputs(opts.Method, opts.Variables, opts.Entities, opts.Ancestor, opts.ChildType, opts.Nodes, opts.ConstrainedEntities, opts.NumEntitiesExistence)
	case "compare":
		if strings.TrimSpace(opts.Baseline) == "" || strings.TrimSpace(opts.Candidate) == "" {
			return fmt.Errorf("baseline and candidate are required in compare mode")
		}
		if opts.RelativeThreshold < 0 || opts.AbsoluteThresholdMS < 0 {
			return fmt.Errorf("comparison thresholds must be non-negative")
		}
		return nil
	default:
		return fmt.Errorf("unsupported mode: %s", opts.Mode)
	}
}

func runCommand(ctx context.Context, opts cliOptions, stdout io.Writer, stderr io.Writer) int {
	configContents, err := os.ReadFile(opts.Config)
	if err != nil {
		fmt.Fprintf(stderr, "Failed to read Spanner config: %v\n", err)
		return exitError
	}

	client, err := spanner.NewSpannerClient(ctx, string(configContents), &spanner.SpannerClientOptions{
		UseMultiEntitySchema: opts.Schema == "multi_entity",
	})
	if err != nil {
		fmt.Fprintf(stderr, "Failed to initialize Spanner client: %v\n", err)
		return exitError
	}
	defer client.Close()

	spec, err := makeQuerySpec(opts)
	if err != nil {
		fmt.Fprintf(stderr, "Invalid query: %v\n", err)
		return exitError
	}
	report, err := newBenchmarkReport(opts, configContents, spec)
	if err != nil {
		fmt.Fprintf(stderr, "Failed to initialize report: %v\n", err)
		return exitError
	}

	runErr := runBenchmark(ctx, client, spec, opts.LogSQL, &report.Cases[0], report.Profile)
	if err := writeReport(opts.Output, report); err != nil {
		fmt.Fprintf(stderr, "Failed to write report: %v\n", err)
		return exitError
	}

	if runErr != nil {
		fmt.Fprintf(stderr, "Benchmark failed: %v\nReport: %s\n", runErr, opts.Output)
		return exitError
	}
	printRunSummary(stdout, report, opts.Output)
	return exitSuccess
}

func validateInputs(method string, variables string, entities string, ancestor string, childType string, nodes string, constrainedEntities string, numEntitiesExistence int) error {
	if numEntitiesExistence < 0 {
		return fmt.Errorf("num_entities_existence must be non-negative")
	}

	switch method {
	case "GetObservations", "CheckVariableExistence":
		if len(splitList(variables)) == 0 || len(splitList(entities)) == 0 {
			return fmt.Errorf("variables and entities are required for method %s", method)
		}
	case "GetObservationsContainedInPlace":
		if len(splitList(variables)) == 0 || strings.TrimSpace(ancestor) == "" || strings.TrimSpace(childType) == "" {
			return fmt.Errorf("variables, ancestor, and child_type are required for method %s", method)
		}
	case "GetStatVarGroupNode":
		if len(splitList(nodes)) == 0 {
			return fmt.Errorf("nodes are required for method %s", method)
		}
	case "GetFilteredStatVarGroupNode", "GetFilteredTopic":
		if len(splitList(nodes)) == 0 || len(splitList(constrainedEntities)) == 0 {
			return fmt.Errorf("nodes and constrained_entities are required for method %s", method)
		}
		if _, _, err := parseConstrainedEntities(constrainedEntities); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported method: %s", method)
	}
	return nil
}

func splitList(value string) []string {
	if value == "" {
		return []string{}
	}
	items := strings.Split(value, ",")
	result := make([]string, 0, len(items))
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item != "" {
			result = append(result, item)
		}
	}
	return result
}

func parseConstrainedEntities(value string) ([]string, string, error) {
	var constrainedPlaces []string
	constrainedImport := ""
	for _, entity := range splitList(value) {
		if strings.HasPrefix(entity, datasetPrefix) || strings.HasPrefix(entity, sourcePrefix) {
			if constrainedImport != "" {
				return nil, "", fmt.Errorf("only one import or source constraint can be specified")
			}
			constrainedImport = entity
			continue
		}
		constrainedPlaces = append(constrainedPlaces, entity)
	}
	return constrainedPlaces, constrainedImport, nil
}
