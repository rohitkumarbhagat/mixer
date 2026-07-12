# Spanner Emulator Tests

The emulator-backed tests in this package exercise Mixer against a hermetic,
seeded Spanner database. They are disabled by default. They require a
GQL-capable Spanner emulator: version 1.5.49 or newer. The currently verified
version is 1.5.55.

## Run locally

Install and start the emulator in one terminal:

```sh
gcloud components install cloud-spanner-emulator
gcloud components update
gcloud components list --show-versions \
  --filter='id=cloud-spanner-emulator'
gcloud emulators spanner start
```

On macOS, start the emulator with Docker:

```sh
docker run -d -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator/emulator
```

The component-list command shows the installed and available emulator versions.
It can also be run from another terminal while the emulator is running; it
reports the installed binary that `gcloud emulators spanner start` launched.
There is no separate emulator runtime-version command. Stop and update the
emulator if the installed version is older than 1.5.49.

Run the tests from another terminal:

```sh
export SPANNER_EMULATOR_HOST=localhost:9010
RUN_SPANNER_EMULATOR_TESTS=true go test ./internal/server/spanner/golden/emulator -count=1 -v
```

The tests fail before creating resources unless `SPANNER_EMULATOR_HOST` uses
the exact host `localhost`. See the official
[Spanner emulator documentation](https://cloud.google.com/spanner/docs/emulator)
for installation and environment setup, and the emulator
[README](https://github.com/GoogleCloudPlatform/cloud-spanner-emulator/blob/master/README.md)
for supported features and limitations.

Without `RUN_SPANNER_EMULATOR_TESTS=true`, pure fixture-loader tests still run
and emulator-backed tests are reported as skipped. Add future emulator test
files directly to this package so they share its `TestMain`; keep their assets
under `testdata`.

## Resource lifecycle

Use one emulator process per developer machine. Concurrent test processes can
share it because each process creates its own randomly named instance and
database. CI should start one emulator process in each isolated emulator-test
job, so separate pull requests never share an emulator process.

The package creates one database, loads the schema and seed data once, and then
shares that immutable database across parallel tests. This differs from the
general per-test-database recommendation because Mixer only issues reads after
setup; it avoids repeated schema creation and seeding while preserving stable
test data.

Add another database in the same instance when a test mutates data or DDL,
needs incompatible seed data, or requires transaction isolation. Use another
instance only for instance-level behavior. Database provisioning is serialized;
read-only tests may run in parallel after setup.

## Schema and seed data

`testdata/schema.sql` contains the complete emulator DDL and is currently the
SDMX-required subset of
`import/pipeline/workflow/ingestion-helper/clients/schema.sql` from the Data
Commons repo. Every statement must begin with `CREATE` and end with a
semicolon, either on the same line or a separate line. Semicolons inside SQL
strings and comments are unsupported; keep explanatory text in this README.

`testdata/seed.sql` contains one static `INSERT` statement per paragraph. Blank
lines delimit statements, and blank lines inside a statement are unsupported.
All nodes, edges, time series, and observations are committed first. A
successful `IngestionHistory` row is then committed with Spanner commit
timestamps so Mixer reads a timestamp containing the complete fixture.

### Update only emulator-test goldens:

```sh
SPANNER_EMULATOR_HOST=localhost:9010 RUN_SPANNER_EMULATOR_TESTS=true GENERATE_GOLDEN=true go test ./internal/server/spanner/golden/emulator -count=1 -v
```

Verify the updated goldens:

```sh
SPANNER_EMULATOR_HOST=localhost:9010 RUN_SPANNER_EMULATOR_TESTS=true go test ./internal/server/spanner/golden/emulator -count=1 -v
```
