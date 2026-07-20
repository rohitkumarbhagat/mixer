# Mixer query regression tester

`tools/perf_test` is a manual local benchmark for checking whether a Mixer query optimization changed direct Spanner-client latency or correctness.

Each run measures one query against one explicitly selected schema and writes a JSON report. Build and run the baseline and candidate separately, then compare their fresh reports.

This initial version is deliberately small. It does not provide suites, concurrency, load generation, API benchmarks, historical storage, or CI gating.

## Measurement model

A run uses one Spanner client for the complete lifecycle:

1. One untimed preflight call for correctness.
2. Five sequential, untimed warm-up calls by default.
3. Thirty sequential measured calls by default, at concurrency one.
4. One correctness check comparing the normalized preflight result with the final measured result.

Only the selected `SpannerClient` method invocation is timed. Result normalization, serialization, hashing, statistics, and report writing happen outside the measured interval.

The comparison uses median latency. A candidate is a regression only when its median is both at least 10% slower and at least 20 ms slower. Material improvements use the symmetric rule. Everything else is a pass.

## Recommended workflow

Build the same tool from separate baseline and candidate worktrees:

```bash
# Run in the baseline worktree.
go build -o /tmp/mixer-perf-baseline ./tools/perf_test

# Run in the candidate worktree.
go build -o /tmp/mixer-perf-candidate ./tools/perf_test
```

Run the identical query, schema, profile, environment label, and exact config contents with each binary:

```bash
/tmp/mixer-perf-baseline \
  -mode=run \
  -schema=multi_entity \
  -method=GetObservations \
  -variables=Count_Person,Count_Household \
  -entities=geoId/06,geoId/08 \
  -warmup=5 \
  -iterations=30 \
  -environment_label=mixer-benchmark-db \
  -config=/path/to/spanner_graph_info.yaml \
  -output=/tmp/baseline.json

/tmp/mixer-perf-candidate \
  -mode=run \
  -schema=multi_entity \
  -method=GetObservations \
  -variables=Count_Person,Count_Household \
  -entities=geoId/06,geoId/08 \
  -warmup=5 \
  -iterations=30 \
  -environment_label=mixer-benchmark-db \
  -config=/path/to/spanner_graph_info.yaml \
  -output=/tmp/candidate.json
```

Compare the reports:

```bash
/tmp/mixer-perf-candidate \
  -mode=compare \
  -baseline=/tmp/baseline.json \
  -candidate=/tmp/candidate.json \
  -relative_threshold=10 \
  -absolute_threshold_ms=20
```

The comparison prints both medians, p90 and p95 values, the absolute and relative median deltas, result summaries, and one of `PASS`, `IMPROVEMENT`, `REGRESSION`, or `INVALID`.

Always create fresh baseline and candidate reports close together on the same machine. If a regression is surprising, rerun in reverse order—candidate first, then baseline—to expose database drift, machine noise, or run-order effects.

## Supported methods

The existing six query operations are supported.

### GetObservations

Requires `-variables` and `-entities`; accepts `-date`.

```bash
go run ./tools/perf_test \
  -mode=run -schema=legacy -method=GetObservations \
  -variables=Count_Person -entities=country/USA \
  -output=/tmp/observations.json
```

### CheckVariableExistence

Requires `-variables` and `-entities`.

```bash
go run ./tools/perf_test \
  -mode=run -schema=legacy -method=CheckVariableExistence \
  -variables=Count_Person -entities=country/USA \
  -output=/tmp/existence.json
```

### GetObservationsContainedInPlace

Requires `-variables`, `-ancestor`, and `-child_type`; accepts `-date`.

```bash
go run ./tools/perf_test \
  -mode=run -schema=multi_entity -method=GetObservationsContainedInPlace \
  -variables=Count_Person -ancestor=country/USA -child_type=State \
  -output=/tmp/contained-in.json
```

### GetStatVarGroupNode

Requires `-nodes`; accepts `-include_definitions`.

```bash
go run ./tools/perf_test \
  -mode=run -schema=legacy -method=GetStatVarGroupNode \
  -nodes=dc/g/Agriculture -include_definitions \
  -output=/tmp/stat-var-group.json
```

### GetFilteredStatVarGroupNode

Requires `-nodes` and `-constrained_entities`; accepts `-num_entities_existence` and `-include_definitions`.

```bash
go run ./tools/perf_test \
  -mode=run -schema=multi_entity -method=GetFilteredStatVarGroupNode \
  -nodes=dc/g/Environment \
  -constrained_entities=country/USA,country/IND \
  -num_entities_existence=2 \
  -output=/tmp/filtered-stat-var-group.json
```

### GetFilteredTopic

Requires `-nodes` and `-constrained_entities`; accepts `-num_entities_existence`.

```bash
go run ./tools/perf_test \
  -mode=run -schema=multi_entity -method=GetFilteredTopic \
  -nodes=dc/topic/Demographics \
  -constrained_entities=dc/s/WorldBank \
  -output=/tmp/filtered-topic.json
```

In filtered queries, `dc/s/...` and `dc/d/...` values are source/import constraints. At most one source/import constraint is allowed; all other values are place constraints.

## Run flags

- `-mode`: `run` or `compare`; defaults to `run`.
- `-schema`: Required in run mode; `legacy` or `multi_entity`.
- `-method`: One of the six methods above; defaults to `GetObservations`.
- `-warmup`: Untimed warm-up calls; defaults to `5` and may be zero.
- `-iterations`: Measured calls; defaults to `30` and must be positive.
- `-output`: Required JSON report path.
- `-name`: Optional case name; defaults to the method name.
- `-environment_label`: Optional label identifying the database/environment.
- `-log_sql`: Logs SQL for the untimed preflight call only. It never enables SQL logging for warm-up or measured calls.
- `-config`: Spanner graph-info YAML; defaults to `deploy/storage/spanner_graph_info.yaml`.
- Query flags: `-variables`, `-entities`, `-nodes`, `-constrained_entities`, `-ancestor`, `-child_type`, `-date`, `-num_entities_existence`, and `-include_definitions`.

Compare mode requires only `-baseline` and `-candidate`; run-specific flags are ignored. Its thresholds default to `-relative_threshold=10` and `-absolute_threshold_ms=20`.

## Report compatibility and correctness

Reports contain one case in a `cases` array and use schema version `1`. The report records the raw samples and statistics, effective query inputs, input and result digests, result counts, schema, profile, Go version, VCS build information, environment label, and a SHA-256 digest of the exact config file bytes. It never stores the config contents or credentials.

Comparison returns `INVALID` unless both reports have:

- Report schema version `1` and exactly one case.
- The same config digest, environment label, explicit schema, measurement profile, and input digest.
- Complete samples and no recorded query error.
- Matching preflight and final result digests within each run.
- The same result digest across baseline and candidate.

Cross-schema reports are intentionally incompatible with the formal regression classification. To inspect legacy and multi-entity performance, run each schema explicitly and review the separate reports; do not mix those results with a code-regression comparison.

## Exit codes

- `0`: `PASS` or `IMPROVEMENT`.
- `1`: Invalid command, report read failure, initialization failure, query failure, or report write failure.
- `2`: `REGRESSION`.
- `3`: `INVALID` comparison because reports or results are incompatible.

## Environment guidance

Use a dedicated real benchmark or staging Spanner database with representative, stable data. Keep the baseline and candidate on the same machine and run them near in time.

The Spanner emulator is useful for functional tests, but emulator timings are not performance evidence. It does not reproduce production service latency, query planning, resource contention, or distributed storage behavior.

Avoid random or fake cache-busting IDs. This tool measures steady-state direct-client latency, not cold starts or API-cache behavior.

## Verification

```bash
GOCACHE=/private/tmp/mixer-perf-go-build-cache \
  go test -count=1 ./tools/perf_test

git diff --check
```
