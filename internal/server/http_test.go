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

package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func mustParseNodeRequest(t *testing.T, textproto string) *pbv2.NodeRequest {
	t.Helper()
	req := &pbv2.NodeRequest{}
	if err := prototext.Unmarshal([]byte(textproto), req); err != nil {
		t.Fatalf("failed to parse NodeRequest textproto: %v", err)
	}
	return req
}

func mustParseNodeResponse(t *testing.T, textproto string) *pbv2.NodeResponse {
	t.Helper()
	resp := &pbv2.NodeResponse{}
	if err := prototext.Unmarshal([]byte(textproto), resp); err != nil {
		t.Fatalf("failed to parse NodeResponse textproto: %v", err)
	}
	return resp
}

func mustParseObservationRequest(t *testing.T, textproto string) *pbv2.ObservationRequest {
	t.Helper()
	req := &pbv2.ObservationRequest{}
	if err := prototext.Unmarshal([]byte(textproto), req); err != nil {
		t.Fatalf("failed to parse ObservationRequest textproto: %v", err)
	}
	return req
}

func mustParseObservationResponse(t *testing.T, textproto string) *pbv2.ObservationResponse {
	t.Helper()
	resp := &pbv2.ObservationResponse{}
	if err := prototext.Unmarshal([]byte(textproto), resp); err != nil {
		t.Fatalf("failed to parse ObservationResponse textproto: %v", err)
	}
	return resp
}

func TestV2NodeHTTPGetBindsRequest(t *testing.T) {
	var gotReq *pbv2.NodeRequest
	var gotCtx context.Context
	handler := newV2NodeHTTPHandler(func(ctx context.Context, req *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
		gotCtx = ctx
		gotReq = proto.Clone(req).(*pbv2.NodeRequest)
		return &pbv2.NodeResponse{}, nil
	})

	req := httptest.NewRequest(http.MethodGet, "/v2/node?nodes=geoId/06&nodes=geoId/11&property=-%3Ename&limit=10&next_token=next", nil)
	req.Header.Set("X-Surface", "website")
	req.Header.Set("X-Remote", "true")
	req.Header.Set("X-Skip-Cache", "true")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}
	want := mustParseNodeRequest(t, `
nodes: "geoId/06"
nodes: "geoId/11"
property: "->name"
limit: 10
next_token: "next"
`)
	if diff := cmp.Diff(want, gotReq, protocmp.Transform()); diff != "" {
		t.Fatalf("request mismatch (-want +got):\n%s", diff)
	}
	md, ok := metadata.FromIncomingContext(gotCtx)
	if !ok {
		t.Fatal("missing metadata")
	}
	if got := md.Get("x-surface"); !reflect.DeepEqual(got, []string{"website"}) {
		t.Fatalf("x-surface metadata = %v", got)
	}
	if got := md.Get("x-remote"); !reflect.DeepEqual(got, []string{"true"}) {
		t.Fatalf("x-remote metadata = %v", got)
	}
	if got := md.Get("x-skip-cache"); !reflect.DeepEqual(got, []string{"true"}) {
		t.Fatalf("x-skip-cache metadata = %v", got)
	}
}

func TestV2NodeHTTPGetAcceptsNextToken(t *testing.T) {
	var gotReq *pbv2.NodeRequest
	handler := newV2NodeHTTPHandler(func(ctx context.Context, req *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
		gotReq = proto.Clone(req).(*pbv2.NodeRequest)
		return &pbv2.NodeResponse{}, nil
	})

	req := httptest.NewRequest(http.MethodGet, "/v2/node?nextToken=next", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}
	want := mustParseNodeRequest(t, `
next_token: "next"
`)
	if diff := cmp.Diff(want, gotReq, protocmp.Transform()); diff != "" {
		t.Fatalf("request mismatch (-want +got):\n%s", diff)
	}
}

func TestV2NodeHTTPPostBindsRequest(t *testing.T) {
	var gotReq *pbv2.NodeRequest
	handler := newV2NodeHTTPHandler(func(ctx context.Context, req *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
		gotReq = proto.Clone(req).(*pbv2.NodeRequest)
		return &pbv2.NodeResponse{}, nil
	})

	body := `{"nodes":["geoId/06"],"property":"->name","limit":10,"nextToken":"next","extra":"ignored"}`
	req := httptest.NewRequest(http.MethodPost, "/v2/node", strings.NewReader(body))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}
	want := mustParseNodeRequest(t, `
nodes: "geoId/06"
property: "->name"
limit: 10
next_token: "next"
`)
	if diff := cmp.Diff(want, gotReq, protocmp.Transform()); diff != "" {
		t.Fatalf("request mismatch (-want +got):\n%s", diff)
	}
}

func TestV2NodeHTTPSuccessWritesProtoJSON(t *testing.T) {
	want := mustParseNodeResponse(t, `
data: {
  key: "geoId/06"
  value: {
    properties: "name"
  }
}
next_token: "next"
`)
	handler := newV2NodeHTTPHandler(func(ctx context.Context, req *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
		return want, nil
	})

	req := httptest.NewRequest(http.MethodGet, "/v2/node", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}
	resp := &pbv2.NodeResponse{}
	if err := protojson.Unmarshal(rr.Body.Bytes(), resp); err != nil {
		t.Fatalf("response is not proto JSON: %v", err)
	}
	if diff := cmp.Diff(want, resp, protocmp.Transform()); diff != "" {
		t.Fatalf("response mismatch (-want +got):\n%s", diff)
	}
	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid response JSON: %v", err)
	}
	if _, ok := body["nextToken"]; !ok {
		t.Fatal("response JSON missing nextToken")
	}
	if _, ok := body["next_token"]; ok {
		t.Fatal("response JSON uses next_token, want nextToken")
	}
}

func TestV2NodeHTTPInvalidLimitReturnsBadRequest(t *testing.T) {
	handler := newV2NodeHTTPHandler(func(ctx context.Context, req *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
		t.Fatal("handler should not be called")
		return nil, nil
	})

	req := httptest.NewRequest(http.MethodGet, "/v2/node?limit=bad", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestV2NodeHTTPUnsupportedMethodReturnsMethodNotAllowed(t *testing.T) {
	handler := newV2NodeHTTPHandler(func(ctx context.Context, req *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
		t.Fatal("handler should not be called")
		return nil, nil
	})

	req := httptest.NewRequest(http.MethodPut, "/v2/node", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusMethodNotAllowed)
	}
}

func TestV2ObservationHTTPGetBindsRequest(t *testing.T) {
	var gotReq *pbv2.ObservationRequest
	handler := newObservationHTTPHandler("/v2/observation", func(ctx context.Context, req *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
		gotReq = proto.Clone(req).(*pbv2.ObservationRequest)
		return &pbv2.ObservationResponse{}, nil
	})

	req := httptest.NewRequest(http.MethodGet, "/v2/observation?variable.dcids=Count_Person&variable.dcids=Count_Farm&entity.expression=geoId/06%3C-containedInPlace%2B%7BtypeOf%3ACounty%7D&date=LATEST&value=10&select=entity&select=value&filter.domains=cdc.gov&filter.facet_ids=123", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}
	want := mustParseObservationRequest(t, `
variable: {
  dcids: "Count_Person"
  dcids: "Count_Farm"
}
entity: {
  expression: "geoId/06<-containedInPlace+{typeOf:County}"
}
date: "LATEST"
value: "10"
filter: {
  domains: "cdc.gov"
  facet_ids: "123"
}
select: "entity"
select: "value"
`)
	if diff := cmp.Diff(want, gotReq, protocmp.Transform()); diff != "" {
		t.Fatalf("request mismatch (-want +got):\n%s", diff)
	}
}

func TestV3ObservationHTTPGetRoutesToV3Caller(t *testing.T) {
	var called bool
	var gotReq *pbv2.ObservationRequest
	handler := newObservationHTTPHandler("/v3/observation", func(ctx context.Context, req *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
		called = true
		gotReq = proto.Clone(req).(*pbv2.ObservationRequest)
		return &pbv2.ObservationResponse{}, nil
	})

	req := httptest.NewRequest(http.MethodGet, "/v3/observation?variable.formula=Count_Person-Count_Person_Female&entity.dcids=geoId/06", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}
	if !called {
		t.Fatal("V3 observation caller was not called")
	}
	want := mustParseObservationRequest(t, `
variable: {
  formula: "Count_Person-Count_Person_Female"
}
entity: {
  dcids: "geoId/06"
}
`)
	if diff := cmp.Diff(want, gotReq, protocmp.Transform()); diff != "" {
		t.Fatalf("request mismatch (-want +got):\n%s", diff)
	}
}

func TestV2ObservationHTTPPostBindsRequest(t *testing.T) {
	var gotReq *pbv2.ObservationRequest
	handler := newObservationHTTPHandler("/v2/observation", func(ctx context.Context, req *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
		gotReq = proto.Clone(req).(*pbv2.ObservationRequest)
		return &pbv2.ObservationResponse{}, nil
	})

	body := `{"variable":{"dcids":["Count_Person"]},"entity":{"dcids":["geoId/06"]},"date":"2020","select":["entity","value"],"extra":"ignored"}`
	req := httptest.NewRequest(http.MethodPost, "/v2/observation", strings.NewReader(body))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}
	want := mustParseObservationRequest(t, `
variable: {
  dcids: "Count_Person"
}
entity: {
  dcids: "geoId/06"
}
date: "2020"
select: "entity"
select: "value"
`)
	if diff := cmp.Diff(want, gotReq, protocmp.Transform()); diff != "" {
		t.Fatalf("request mismatch (-want +got):\n%s", diff)
	}
}

func TestV2ObservationHTTPSuccessWritesProtoJSON(t *testing.T) {
	want := mustParseObservationResponse(t, `
by_variable: {
  key: "Count_Person"
  value: {
    by_entity: {
      key: "geoId/06"
      value: {
        ordered_facets: {
          facet_id: "123"
          observations: {
            date: "2020"
            value: 39.5
          }
          obs_count: 1
          earliest_date: "2020"
          latest_date: "2020"
        }
      }
    }
  }
}
facets: {
  key: "123"
  value: {
    import_name: "test"
    provenance_url: "https://example.org"
  }
}
`)
	handler := newObservationHTTPHandler("/v2/observation", func(ctx context.Context, req *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
		return want, nil
	})

	req := httptest.NewRequest(http.MethodGet, "/v2/observation", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}
	resp := &pbv2.ObservationResponse{}
	if err := protojson.Unmarshal(rr.Body.Bytes(), resp); err != nil {
		t.Fatalf("response is not proto JSON: %v", err)
	}
	if diff := cmp.Diff(want, resp, protocmp.Transform()); diff != "" {
		t.Fatalf("response mismatch (-want +got):\n%s", diff)
	}
	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid response JSON: %v", err)
	}
	if _, ok := body["byVariable"]; !ok {
		t.Fatal("response JSON missing byVariable")
	}
	if _, ok := body["by_variable"]; ok {
		t.Fatal("response JSON uses by_variable, want byVariable")
	}
}

func TestV2ObservationHTTPUnsupportedMethodReturnsMethodNotAllowed(t *testing.T) {
	handler := newObservationHTTPHandler("/v2/observation", func(ctx context.Context, req *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
		t.Fatal("handler should not be called")
		return nil, nil
	})

	req := httptest.NewRequest(http.MethodPut, "/v2/observation", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusMethodNotAllowed)
	}
}

func TestHTTPHandlerUnknownPathReturnsNotFound(t *testing.T) {
	handler := (&Server{}).HTTPHandler()

	req := httptest.NewRequest(http.MethodGet, "/v2/missing", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusNotFound)
	}
}

func TestV2ObservationHTTPGRPCInvalidArgumentReturnsBadRequest(t *testing.T) {
	handler := newObservationHTTPHandler("/v2/observation", func(ctx context.Context, req *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
		return nil, status.Error(codes.InvalidArgument, "bad request")
	})

	req := httptest.NewRequest(http.MethodGet, "/v2/observation", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
	var body httpErrorResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid error JSON: %v", err)
	}
	if body.Code != int(codes.InvalidArgument) || body.Message != "bad request" {
		t.Fatalf("body = %+v", body)
	}
}

func TestV2NodeHTTPGRPCInvalidArgumentReturnsBadRequest(t *testing.T) {
	handler := newV2NodeHTTPHandler(func(ctx context.Context, req *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
		return nil, status.Error(codes.InvalidArgument, "bad request")
	})

	req := httptest.NewRequest(http.MethodGet, "/v2/node", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
	var body httpErrorResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid error JSON: %v", err)
	}
	if body.Code != int(codes.InvalidArgument) || body.Message != "bad request" {
		t.Fatalf("body = %+v", body)
	}
}
