// Copyright 2024 Google LLC
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

// Model objects related to the SQL database.
package sqldb

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Observation struct to represent a row in the observations table
type Observation struct {
	Entity            string  `db:"entity"`
	Variable          string  `db:"variable"`
	Date              string  `db:"date"`
	Value             float64 `db:"value"`
	Provenance        string  `db:"provenance"`
	Unit              string  `db:"unit"`
	ScalingFactor     string  `db:"scaling_factor"`
	MeasurementMethod string  `db:"measurement_method"`
	ObservationPeriod string  `db:"observation_period"`
	Properties        string  `db:"properties"`
}

// Triple represents a row in the triples table
type Triple struct {
	SubjectID   string `db:"subject_id"`
	Predicate   string `db:"predicate"`
	ObjectID    string `db:"object_id"`
	ObjectValue string `db:"object_value"`
}

// SVSummary represents a SV summary row.
type SVSummary struct {
	Variable        string      `db:"variable"`
	EntityType      string      `db:"entity_type"`
	EntityCount     int32       `db:"entity_count"`
	MinValue        float64     `db:"min_value"`
	MaxValue        float64     `db:"max_value"`
	SampleEntityIds StringSlice `db:"sample_entity_ids"`
}

// StringSlice is a custom scanner for comma-separated strings.
type StringSlice []string

// Scan implements the sql.Scanner interface and decodes a comma-separated string field into a StringSlice ([]string) value.
func (s *StringSlice) Scan(src interface{}) error {
	if src == nil {
		*s = []string{}
		return nil
	}

	var val string

	switch v := src.(type) {
	case []byte:
		val = string(v)
	case string:
		val = v
	default:
		return fmt.Errorf("failed to decode []string: type = %T, value = %v", src, src)
	}

	*s = strings.Split(val, ",")
	return nil
}

// StatVarGroup represents a StatVarGroup row.
type StatVarGroup struct {
	ID       string `db:"svg_id"`
	Name     string `db:"svg_name"`
	ParentID string `db:"svg_parent_id"`
}

// StatisticalVariable represents a StatisticalVariable row.
type StatisticalVariable struct {
	ID          string `db:"sv_id"`
	Name        string `db:"sv_name"`
	SVGID       string `db:"svg_id"`
	Description string `db:"sv_description"`
}

// EntityCount represents a row that includes the count of entities for a given variable, date and provenance.
type EntityCount struct {
	Variable   string `db:"variable"`
	Date       string `db:"date"`
	Provenance string `db:"provenance"`
	Count      int    `db:"num_entities"`
}

// Node represents a row that represents a (node, predicate) pair.
// The node is a dcid that represents either a subject or an object.
type NodePredicate struct {
	Node      string `db:"node"`
	Predicate string `db:"predicate"`
}

// SubjectObject represents a row for (subject_id, object_id) pairs.
type SubjectObject struct {
	SubjectID string `db:"subject_id"`
	ObjectID  string `db:"object_id"`
}

// EntityVariables represents a row that includes an entity and its variables.
type EntityVariables struct {
	Entity    string      `db:"entity"`
	Variables StringSlice `db:"variables"`
}

// ObservationCount represents a row that includes the count of observations for a given entity and variable.
type ObservationCount struct {
	Entity   string `db:"entity"`
	Variable string `db:"variable"`
	Count    int    `db:"num_obs"`
}

// EntityVariable represents a row that includes an entity and a variable.
type EntityVariable struct {
	Entity   string `db:"entity"`
	Variable string `db:"variable"`
}

// ProvenanceInfo represents a row that includes provenance info (id, name, URL).
type ProvenanceInfo struct {
	ID   string `db:"provenance_id"`
	Name string `db:"provenance_name"`
	URL  string `db:"provenance_url"`
}

// ImportMetadata represents the metadata column in the imports table.
type ImportMetadata struct {
	NumObs  *int32 `json:"numObs"`
	NumVars *int32 `json:"numVars"`
}

// Scan implements the sql.Scanner interface for ImportMetadata.
func (md *ImportMetadata) Scan(src interface{}) error {
	if src == nil {
		return nil
	}

	var data []byte

	switch v := src.(type) {
	case []byte:
		data = v
	case string:
		data = []byte(v)
	default:
		return fmt.Errorf("failed to decode ImportMetadata: type = %T, value = %v", src, src)
	}

	return json.Unmarshal(data, md)
}

// ImportInfo represents a row in the imports table
type ImportInfo struct {
	ImportedAt string         `db:"imported_at"`
	Status     string         `db:"status"`
	Metadata   ImportMetadata `db:"metadata"`
}
