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

func TestHTTPHandlerUnknownPathReturnsNotFound(t *testing.T) {
	handler := (&Server{}).HTTPHandler()

	req := httptest.NewRequest(http.MethodGet, "/v2/observation", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusNotFound)
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
