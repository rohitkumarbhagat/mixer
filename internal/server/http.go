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
	"io"
	"net/http"
	"strconv"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

type v2NodeCaller func(context.Context, *pbv2.NodeRequest) (*pbv2.NodeResponse, error)

type httpErrorResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// HTTPHandler returns the native HTTP API handler for migrated endpoints.
func (s *Server) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/v2/node", newV2NodeHTTPHandler(s.V2Node))
	return mux
}

func newV2NodeHTTPHandler(call v2NodeCaller) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/node" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			w.Header().Set("Allow", "GET, POST")
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		req, err := parseV2NodeHTTPRequest(r)
		if err != nil {
			writeHTTPError(w, err)
			return
		}
		resp, err := call(contextWithHTTPMetadata(r.Context(), r.Header), req)
		if err != nil {
			writeHTTPError(w, err)
			return
		}
		body, err := protojson.Marshal(resp)
		if err != nil {
			writeHTTPError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		//nolint:errcheck // Client write failures cannot be recovered here.
		w.Write(body)
	})
}

func parseV2NodeHTTPRequest(r *http.Request) (*pbv2.NodeRequest, error) {
	if r.Method == http.MethodPost {
		req := &pbv2.NodeRequest{}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(body, req); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return req, nil
	}

	values := r.URL.Query()
	req := &pbv2.NodeRequest{
		Nodes:     values["nodes"],
		Property:  values.Get("property"),
		NextToken: values.Get("next_token"),
	}
	if req.NextToken == "" {
		req.NextToken = values.Get("nextToken")
	}
	if limit := values.Get("limit"); limit != "" {
		parsedLimit, err := strconv.ParseInt(limit, 10, 32)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid limit: %s", limit)
		}
		req.Limit = int32(parsedLimit)
	}
	return req, nil
}

func contextWithHTTPMetadata(ctx context.Context, header http.Header) context.Context {
	pairs := []string{}
	if value := header.Get("X-Surface"); value != "" {
		pairs = append(pairs, "x-surface", value)
	}
	if value := header.Get("X-Remote"); value != "" {
		pairs = append(pairs, "x-remote", value)
	}
	if value := header.Get("X-Skip-Cache"); value != "" {
		pairs = append(pairs, "x-skip-cache", value)
	}
	if len(pairs) == 0 {
		return ctx
	}
	return metadata.NewIncomingContext(ctx, metadata.Pairs(pairs...))
}

func writeHTTPError(w http.ResponseWriter, err error) {
	st, _ := status.FromError(err)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatusFromGRPCCode(st.Code()))
	//nolint:errcheck // Client write failures cannot be recovered here.
	json.NewEncoder(w).Encode(httpErrorResponse{
		Code:    int(st.Code()),
		Message: st.Message(),
	})
}

func httpStatusFromGRPCCode(code codes.Code) int {
	switch code {
	case codes.InvalidArgument, codes.FailedPrecondition, codes.OutOfRange:
		return http.StatusBadRequest
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.NotFound:
		return http.StatusNotFound
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}
