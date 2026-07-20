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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/datacommonsorg/mixer/internal/server/spanner"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/metadata"
)

const reportSchemaVersion = 1

type queryClient interface {
	GetObservations(context.Context, []string, []string, string) ([]*spanner.Observation, error)
	CheckVariableExistence(context.Context, []string, []string) ([][]string, error)
	GetObservationsContainedInPlace(context.Context, []string, *v2.ContainedInPlace, string) ([]*spanner.Observation, error)
	GetStatVarGroupNode(context.Context, []string, bool) ([]*spanner.StatVarGroupNode, error)
	GetFilteredStatVarGroupNode(context.Context, []string, []string, string, int, bool) (map[string]*spanner.FilteredStatVarGroupNode, error)
	GetFilteredTopic(context.Context, []string, []string, string, int) (map[string]int, error)
}

type queryInputs struct {
	Variables            []string `json:"variables,omitempty"`
	Entities             []string `json:"entities,omitempty"`
	Date                 *string  `json:"date,omitempty"`
	Ancestor             *string  `json:"ancestor,omitempty"`
	ChildType            *string  `json:"child_type,omitempty"`
	Nodes                []string `json:"nodes,omitempty"`
	ConstrainedPlaces    []string `json:"constrained_places,omitempty"`
	ConstrainedImport    *string  `json:"constrained_import,omitempty"`
	NumEntitiesExistence *int     `json:"num_entities_existence,omitempty"`
	IncludeDefinitions   *bool    `json:"include_definitions,omitempty"`
}

type querySpec struct {
	Name   string
	Method string
	Inputs queryInputs
}

type benchmarkReport struct {
	SchemaVersion    int              `json:"schema_version"`
	GeneratedAt      time.Time        `json:"generated_at"`
	VCS              vcsInfo          `json:"vcs"`
	GoVersion        string           `json:"go_version"`
	EnvironmentLabel string           `json:"environment_label"`
	ConfigDigest     string           `json:"config_digest"`
	Schema           string           `json:"schema"`
	Profile          benchmarkProfile `json:"profile"`
	Cases            []caseReport     `json:"cases"`
}

type vcsInfo struct {
	Revision string `json:"revision,omitempty"`
	Modified *bool  `json:"modified,omitempty"`
}

type benchmarkProfile struct {
	Warmup      int `json:"warmup"`
	Iterations  int `json:"iterations"`
	Concurrency int `json:"concurrency"`
}

type caseReport struct {
	Name                  string           `json:"name"`
	Method                string           `json:"method"`
	Inputs                queryInputs      `json:"inputs"`
	InputDigest           string           `json:"input_digest"`
	PreflightResultDigest string           `json:"preflight_result_digest,omitempty"`
	FinalResultDigest     string           `json:"final_result_digest,omitempty"`
	ResultSummary         map[string]int64 `json:"result_summary,omitempty"`
	SamplesMS             []float64        `json:"samples_ms"`
	Stats                 latencyStats     `json:"stats"`
	Error                 *benchmarkError  `json:"error,omitempty"`
}

type latencyStats struct {
	Count    int     `json:"count"`
	MinMS    float64 `json:"min_ms"`
	MeanMS   float64 `json:"mean_ms"`
	MedianMS float64 `json:"median_ms"`
	P90MS    float64 `json:"p90_ms"`
	P95MS    float64 `json:"p95_ms"`
	MaxMS    float64 `json:"max_ms"`
}

type benchmarkError struct {
	Phase     string `json:"phase"`
	Iteration int    `json:"iteration,omitempty"`
	Message   string `json:"message"`
}

func makeQuerySpec(opts cliOptions) (querySpec, error) {
	name := opts.Name
	if name == "" {
		name = opts.Method
	}
	spec := querySpec{Name: name, Method: opts.Method}
	date := opts.Date

	switch opts.Method {
	case "GetObservations":
		spec.Inputs = queryInputs{Variables: splitList(opts.Variables), Entities: splitList(opts.Entities), Date: &date}
	case "CheckVariableExistence":
		spec.Inputs = queryInputs{Variables: splitList(opts.Variables), Entities: splitList(opts.Entities)}
	case "GetObservationsContainedInPlace":
		ancestor := strings.TrimSpace(opts.Ancestor)
		childType := strings.TrimSpace(opts.ChildType)
		spec.Inputs = queryInputs{Variables: splitList(opts.Variables), Date: &date, Ancestor: &ancestor, ChildType: &childType}
	case "GetStatVarGroupNode":
		includeDefinitions := opts.IncludeDefinitions
		spec.Inputs = queryInputs{Nodes: splitList(opts.Nodes), IncludeDefinitions: &includeDefinitions}
	case "GetFilteredStatVarGroupNode", "GetFilteredTopic":
		places, constrainedImport, err := parseConstrainedEntities(opts.ConstrainedEntities)
		if err != nil {
			return querySpec{}, err
		}
		numEntities := opts.NumEntitiesExistence
		spec.Inputs = queryInputs{
			Nodes:                splitList(opts.Nodes),
			ConstrainedPlaces:    places,
			ConstrainedImport:    &constrainedImport,
			NumEntitiesExistence: &numEntities,
		}
		if opts.Method == "GetFilteredStatVarGroupNode" {
			includeDefinitions := opts.IncludeDefinitions
			spec.Inputs.IncludeDefinitions = &includeDefinitions
		}
	default:
		return querySpec{}, fmt.Errorf("unsupported method: %s", opts.Method)
	}
	return spec, nil
}

func newBenchmarkReport(opts cliOptions, configContents []byte, spec querySpec) (benchmarkReport, error) {
	inputDigest, err := digestJSON(struct {
		Name   string      `json:"name"`
		Schema string      `json:"schema"`
		Method string      `json:"method"`
		Inputs queryInputs `json:"inputs"`
	}{spec.Name, opts.Schema, spec.Method, spec.Inputs})
	if err != nil {
		return benchmarkReport{}, err
	}

	configHash := sha256.Sum256(configContents)
	return benchmarkReport{
		SchemaVersion:    reportSchemaVersion,
		GeneratedAt:      time.Now().UTC(),
		VCS:              readVCSInfo(),
		GoVersion:        runtime.Version(),
		EnvironmentLabel: opts.EnvironmentLabel,
		ConfigDigest:     hex.EncodeToString(configHash[:]),
		Schema:           opts.Schema,
		Profile: benchmarkProfile{
			Warmup:      opts.Warmup,
			Iterations:  opts.Iterations,
			Concurrency: 1,
		},
		Cases: []caseReport{{
			Name:        spec.Name,
			Method:      spec.Method,
			Inputs:      spec.Inputs,
			InputDigest: inputDigest,
			SamplesMS:   []float64{},
		}},
	}, nil
}

func readVCSInfo() vcsInfo {
	info := vcsInfo{}
	buildInfo, ok := debug.ReadBuildInfo()
	if !ok {
		return info
	}
	for _, setting := range buildInfo.Settings {
		switch setting.Key {
		case "vcs.revision":
			info.Revision = setting.Value
		case "vcs.modified":
			if modified, err := strconv.ParseBool(setting.Value); err == nil {
				info.Modified = &modified
			}
		}
	}
	return info
}

func executeQuery(ctx context.Context, client queryClient, spec querySpec) (any, error) {
	in := spec.Inputs
	switch spec.Method {
	case "GetObservations":
		return client.GetObservations(ctx, in.Variables, in.Entities, *in.Date)
	case "CheckVariableExistence":
		return client.CheckVariableExistence(ctx, in.Variables, in.Entities)
	case "GetObservationsContainedInPlace":
		return client.GetObservationsContainedInPlace(ctx, in.Variables, &v2.ContainedInPlace{
			Ancestor:       *in.Ancestor,
			ChildPlaceType: *in.ChildType,
		}, *in.Date)
	case "GetStatVarGroupNode":
		return client.GetStatVarGroupNode(ctx, in.Nodes, *in.IncludeDefinitions)
	case "GetFilteredStatVarGroupNode":
		return client.GetFilteredStatVarGroupNode(ctx, in.Nodes, in.ConstrainedPlaces, *in.ConstrainedImport, *in.NumEntitiesExistence, *in.IncludeDefinitions)
	case "GetFilteredTopic":
		return client.GetFilteredTopic(ctx, in.Nodes, in.ConstrainedPlaces, *in.ConstrainedImport, *in.NumEntitiesExistence)
	default:
		return nil, fmt.Errorf("unsupported method: %s", spec.Method)
	}
}

func runBenchmark(ctx context.Context, client queryClient, spec querySpec, logSQL bool, result *caseReport, profile benchmarkProfile) error {
	preflightContext := ctx
	if logSQL {
		preflightContext = metadata.NewIncomingContext(ctx, metadata.Pairs(util.XLogSQL, "true"))
	}
	preflight, err := executeQuery(preflightContext, client, spec)
	if err != nil {
		return setBenchmarkError(result, "preflight", 1, err)
	}
	preflightDigest, summary, err := normalizeAndDigest(spec.Method, preflight)
	if err != nil {
		return setBenchmarkError(result, "preflight", 1, err)
	}
	result.PreflightResultDigest = preflightDigest
	result.ResultSummary = summary

	for i := 1; i <= profile.Warmup; i++ {
		if _, err := executeQuery(ctx, client, spec); err != nil {
			return setBenchmarkError(result, "warmup", i, err)
		}
	}

	var final any
	for i := 1; i <= profile.Iterations; i++ {
		start := time.Now()
		value, err := executeQuery(ctx, client, spec)
		elapsed := time.Since(start)
		if err != nil {
			result.Stats = calculateStats(result.SamplesMS)
			return setBenchmarkError(result, "measurement", i, err)
		}
		result.SamplesMS = append(result.SamplesMS, float64(elapsed)/float64(time.Millisecond))
		final = value
	}
	result.Stats = calculateStats(result.SamplesMS)

	finalDigest, finalSummary, err := normalizeAndDigest(spec.Method, final)
	if err != nil {
		return setBenchmarkError(result, "correctness", 0, err)
	}
	result.FinalResultDigest = finalDigest
	result.ResultSummary = finalSummary
	if result.PreflightResultDigest != result.FinalResultDigest {
		return setBenchmarkError(result, "correctness", 0, fmt.Errorf("preflight and final result digests differ"))
	}
	return nil
}

func setBenchmarkError(result *caseReport, phase string, iteration int, err error) error {
	result.Error = &benchmarkError{Phase: phase, Iteration: iteration, Message: err.Error()}
	return err
}

func normalizeAndDigest(method string, value any) (string, map[string]int64, error) {
	var summary map[string]int64
	switch method {
	case "GetObservations", "GetObservationsContainedInPlace":
		observations, ok := value.([]*spanner.Observation)
		if !ok {
			return "", nil, fmt.Errorf("unexpected %s result type %T", method, value)
		}
		if observations == nil {
			observations = []*spanner.Observation{}
		}
		for _, observation := range observations {
			if observation == nil {
				continue
			}
			sort.SliceStable(observation.Observations, func(i, j int) bool {
				left, right := observation.Observations[i], observation.Observations[j]
				if left == nil || right == nil {
					return left != nil
				}
				if left.Date != right.Date {
					return left.Date < right.Date
				}
				return left.Value < right.Value
			})
		}
		sort.SliceStable(observations, func(i, j int) bool {
			left, right := observations[i], observations[j]
			if left == nil || right == nil {
				return left != nil
			}
			if left.VariableMeasured != right.VariableMeasured {
				return left.VariableMeasured < right.VariableMeasured
			}
			if left.ObservationAbout != right.ObservationAbout {
				return left.ObservationAbout < right.ObservationAbout
			}
			return left.FacetId < right.FacetId
		})
		var points int64
		for _, observation := range observations {
			if observation != nil {
				points += int64(len(observation.Observations))
			}
		}
		summary = map[string]int64{"rows": int64(len(observations)), "observation_points": points}
		value = observations
	case "CheckVariableExistence":
		pairs, ok := value.([][]string)
		if !ok {
			return "", nil, fmt.Errorf("unexpected %s result type %T", method, value)
		}
		if pairs == nil {
			pairs = [][]string{}
		}
		slices.SortFunc(pairs, func(left, right []string) int { return slices.Compare(left, right) })
		summary = map[string]int64{"pairs": int64(len(pairs))}
		value = pairs
	case "GetStatVarGroupNode":
		nodes, ok := value.([]*spanner.StatVarGroupNode)
		if !ok {
			return "", nil, fmt.Errorf("unexpected %s result type %T", method, value)
		}
		if nodes == nil {
			nodes = []*spanner.StatVarGroupNode{}
		}
		sort.SliceStable(nodes, func(i, j int) bool {
			left, right := nodes[i], nodes[j]
			if left == nil || right == nil {
				return left != nil
			}
			if left.SVG != right.SVG {
				return left.SVG < right.SVG
			}
			return left.SubjectID < right.SubjectID
		})
		summary = map[string]int64{"rows": int64(len(nodes))}
		value = nodes
	case "GetFilteredStatVarGroupNode":
		nodes, ok := value.(map[string]*spanner.FilteredStatVarGroupNode)
		if !ok {
			return "", nil, fmt.Errorf("unexpected %s result type %T", method, value)
		}
		if nodes == nil {
			nodes = map[string]*spanner.FilteredStatVarGroupNode{}
		}
		var svgChildren, statVarChildren, childSVGs int64
		for _, node := range nodes {
			if node == nil {
				continue
			}
			sort.SliceStable(node.SVGChild, func(i, j int) bool { return subjectID(node.SVGChild[i]) < subjectID(node.SVGChild[j]) })
			sort.SliceStable(node.ChildSV, func(i, j int) bool { return childSVSubjectID(node.ChildSV[i]) < childSVSubjectID(node.ChildSV[j]) })
			sort.SliceStable(node.ChildSVG, func(i, j int) bool { return childSVGSubjectID(node.ChildSVG[i]) < childSVGSubjectID(node.ChildSVG[j]) })
			svgChildren += int64(len(node.SVGChild))
			statVarChildren += int64(len(node.ChildSV))
			childSVGs += int64(len(node.ChildSVG))
		}
		summary = map[string]int64{
			"nodes":             int64(len(nodes)),
			"svg_children":      svgChildren,
			"stat_var_children": statVarChildren,
			"child_svgs":        childSVGs,
		}
		value = nodes
	case "GetFilteredTopic":
		nodes, ok := value.(map[string]int)
		if !ok {
			return "", nil, fmt.Errorf("unexpected %s result type %T", method, value)
		}
		if nodes == nil {
			nodes = map[string]int{}
		}
		var children int64
		for _, count := range nodes {
			children += int64(count)
		}
		summary = map[string]int64{"nodes": int64(len(nodes)), "total_children": children}
		value = nodes
	default:
		return "", nil, fmt.Errorf("unsupported method: %s", method)
	}

	digest, err := digestJSON(value)
	return digest, summary, err
}

func subjectID(value *spanner.SVGChild) string {
	if value == nil {
		return ""
	}
	return value.SubjectID
}

func childSVSubjectID(value *spanner.ChildSV) string {
	if value == nil {
		return ""
	}
	return value.SubjectID
}

func childSVGSubjectID(value *spanner.ChildSVG) string {
	if value == nil {
		return ""
	}
	return value.SubjectID
}

func digestJSON(value any) (string, error) {
	contents, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(contents)
	return hex.EncodeToString(hash[:]), nil
}

func calculateStats(samples []float64) latencyStats {
	stats := latencyStats{Count: len(samples)}
	if len(samples) == 0 {
		return stats
	}
	ordered := slices.Clone(samples)
	slices.Sort(ordered)
	stats.MinMS = ordered[0]
	stats.MaxMS = ordered[len(ordered)-1]
	for _, sample := range samples {
		stats.MeanMS += sample
	}
	stats.MeanMS /= float64(len(samples))
	middle := len(ordered) / 2
	if len(ordered)%2 == 0 {
		stats.MedianMS = (ordered[middle-1] + ordered[middle]) / 2
	} else {
		stats.MedianMS = ordered[middle]
	}
	stats.P90MS = nearestRank(ordered, 0.90)
	stats.P95MS = nearestRank(ordered, 0.95)
	return stats
}

func nearestRank(ordered []float64, percentile float64) float64 {
	index := int(math.Ceil(percentile*float64(len(ordered)))) - 1
	return ordered[index]
}

func writeReport(path string, report benchmarkReport) error {
	contents, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	contents = append(contents, '\n')
	if err := os.WriteFile(path, contents, 0644); err != nil {
		return err
	}
	return os.Chmod(path, 0644)
}

func printRunSummary(output io.Writer, report benchmarkReport, path string) {
	result := report.Cases[0]
	fmt.Fprintf(output, "Benchmark complete: %s (%s)\n", result.Name, report.Schema)
	fmt.Fprintf(output, "median=%.3f ms p90=%.3f ms p95=%.3f ms samples=%d\n", result.Stats.MedianMS, result.Stats.P90MS, result.Stats.P95MS, result.Stats.Count)
	fmt.Fprintf(output, "result=%s summary=%s\n", result.FinalResultDigest, formatSummary(result.ResultSummary))
	fmt.Fprintf(output, "report=%s\n", path)
}

func formatSummary(summary map[string]int64) string {
	if len(summary) == 0 {
		return "{}"
	}
	keys := make([]string, 0, len(summary))
	for key := range summary {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", key, summary[key]))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}
