# Server-side queries and Grafana alerting (fork)

This fork adds a backend `QueryData` implementation so Grafana Alerting (and
any other server-side evaluation, such as recording rules or `/api/ds/query`)
can query Huawei Cloud Eye (CES). `src/plugin.json` sets `"alerting": true`.

The existing dashboard path is unchanged. The frontend `DataSource.query()`
override still calls the `/metric-data` resource endpoint, so dashboards, Explore
and the template-variable dashboards keep their current behaviour. Only
backend-evaluated requests use `QueryData`.

Code: `pkg/plugin/query.go` (implementation), `pkg/plugin/query_test.go`
(unit tests), `newSafeCESClient` in `pkg/plugin/client.go`.

## Query model

Alert queries use the same JSON model as the query editor (`src/types.ts`):

| Field        | Required | Default | Accepted values |
|--------------|----------|---------|-----------------|
| `region`     | no       | see below | A CES region ID, for example `af-south-1` |
| `namespace`  | yes      |         | For example `SYS.ECS` |
| `dimstr`     | yes      |         | `name:value[,name:value...]`, 1 to 4 dimensions, for example `instance_id:abc` |
| `metricName` | yes      |         | For example `cpu_util` |
| `filter`     | no       | `average` | `average`, `max`, `min`, `sum`, `variance` |
| `period`     | no       | `1`     | `1`, `300`, `1200`, `3600`, `14400`, `86400` (string or number) |

**Region default.** The query editor shows `cn-east-3` but only saves a region
when the user changes it, and the dashboard path sends
`target.region || 'cn-east-3'`. An empty or missing region therefore defaults
to the configured datasource region in Specific Region Mode (explicit CES
endpoint and project ID, with a region set), and to `cn-east-3` otherwise. The
defaulted value is then validated like an explicit one.

**Periods.** The accepted periods are those of the pinned SDK model
(huaweicloud-sdk-go-v3 v0.1.92, `BatchListMetricDataRequestBody.Period`). The
current CES API page also lists `60`, but this backend rejects it: the pinned
SDK and the integration mock do not support it.

The time range is the alert rule's query time range from the backend request
(`DataQuery.TimeRange`), sent to CES as Unix milliseconds.

## Response format

- One time-series frame per query (`timeseries-multi`), with `frame.RefID` set
  to the query's ref ID.
- The fields are `Time` and a nullable float64 value field named after the
  metric. The value field is labelled `namespace=<namespace>` plus one label per
  dimension, the same labels the dashboard path uses.
- Datapoints are sorted by timestamp. Duplicate timestamps keep the last value,
  and timestamps of 0 or less are dropped. Millisecond precision is kept.
- A missing statistic becomes a null value; it is never replaced by `0`, and
  another statistic is never substituted (except for `period=1`, see below).
- **No data:** the query returns a successful response with **no frames** when
  any of these is true:
  - CES returns no series for the metric;
  - the series has no datapoints;
  - every datapoint is null for the requested statistic;
  - every datapoint has an invalid timestamp.

  Grafana alerting evaluates this as *No Data*, so configure the rule's No Data
  handling. A zero-row frame with labelled fields is deliberately not returned:
  Grafana reduces it to a labelled NaN series and the rule stays *Normal*.
  Series with some null values keep those nulls, and the rule's reducer mode
  decides how they are handled.
- **Errors are per query.** An invalid query, or a failed CES request, sets an
  error on the affected ref IDs only; other queries in the same request still
  return data. Errors never include the access key or secret key. Statuses:

  | Cause | Status |
  |-------|--------|
  | Invalid query or settings, or an unsupported region | 400 |
  | CES responds 401, 403, 404 or 429 | Passed through |
  | Query deadline exceeded, HTTP client timeout (`net.Error.Timeout()`), or CES 408 | 504 |
  | Any other failure (CES 5xx, connection errors, client construction such as the IAM project lookup) | 502 |

## Alert query limitations

- **Dashboard variables are not resolved.** Grafana does not interpolate
  template variables for alert queries, and this plugin's frontend applies its
  variables itself, outside Grafana's `$var` mechanism:
  - Any field containing `$var`, `${var}` or `[[var]]` is rejected with a clear
    error.
  - The bundled dashboard templates (`src/templates/*.json`) store placeholder
    literals rather than `$` syntax: `"region": "-"`, all-zero `dimstr` values
    such as `instance_id:00000000-0000-0000-0000-000000000000`, and
    `"filter": "average"`, `"period": "1"`. On dashboards, the frontend
    overrides these from the `region`, `filter`, `period` and dimension
    variables.
  - An alert copied from a template panel sends the saved literals. Region `"-"`
    is rejected as a template placeholder. Placeholder dimensions are not
    detected and return *No Data*.

  Use explicit values in alert queries.
- **One explicit metric per query.** Wildcards, multi-resource dimension
  selection and dimension-only queries are not supported. Write one query per
  resource, or one rule per resource.
- **Check the dimension.** The query editor displays `defaultQuery`
  (`dimstr: instance_id,1b67...`) until a dimension is picked. That default has
  no `:`, so it is rejected as invalid; select a dimension explicitly.
- **Specific Region Mode.** When the datasource has a CES endpoint, a project ID
  and a region configured, an *explicitly set* query region must match the
  configured region. Otherwise the query fails, instead of silently querying the
  configured endpoint. A missing region defaults to the configured region.
- **Huawei Cloud (multi-region) mode.** Unknown region IDs return
  `unsupported CES region "<id>"` (400); they do not panic. Building the client
  performs an IAM project lookup (cached by the SDK). It runs under the query
  deadline, and its failures return 502.
- **CES time-range caps.** These caps come from the Huawei Cloud API reference
  [Querying Monitoring Data in Batches](https://support.huaweicloud.com/intl/en-us/api-ces/ces_03_0034.html)
  (updated 2026-10-21), `from` parameter. For example: "When period is set to 1,
  if (to – from) > 4 x 3,600 x 1,000, adjust the value of from to to – 4 x
  3,600 x 1,000."

  | Period | Maximum `to - from` |
  |--------|---------------------|
  | `1` | 4h |
  | `300` | 24h |
  | `1200` | 3d |
  | `3600` | 10d |
  | `14400` | 30d |
  | `86400` | 180d |

  CES silently moves `from` forward when a cap is exceeded, so longer ranges are
  rejected up front with a validation error.
- **Batching.** The same page states "Number of metrics x (to-from
  value)/Monitoring period ≤ 3000" and that `from` is adjusted otherwise. It
  also limits a request to 1 to 500 metrics, each with 1 to 4 dimensions.
  - Queries that share region, filter, period and time range are sent in one
    `BatchListMetricData` request, within those limits.
  - The datapoint estimate uses 60s for raw (`period=1`) data. If a raw metric
    reports more often than once a minute, the estimate can be optimistic, so
    prefer an aggregated period for long ranges.
  - Identical metrics are requested once and returned to every ref ID that
    asked for them.
  - Responses are matched to ref IDs by namespace, metric name and dimension
    set; response order is never assumed. Unrequested series are ignored.
- **`period=1` (raw data).** The API reference says: "When period is set to 1
  (original value), the filter field does not take effect." The backend reads
  the requested statistic and falls back to `average` when that is missing. For
  aggregated periods, no fallback is applied.
- **Aggregation lag.** CES aggregates into the start of each period, and the
  API reference recommends `from` at least one period before the current time.
  Use a rule time range of at least two periods (for example `now-10m` for
  `period=300`), or the newest bucket can be empty and the rule can flap to
  *No Data*.
- **Timeouts and rate limits.** A whole `QueryData` call is bounded at 30s. The
  SDK has no context support, so the CES client's HTTP timeout is set to the time
  remaining before the query deadline when the client is created. Cancellation
  returns promptly, but an in-flight SDK call may continue until its HTTP timeout.
  Up to 4 CES requests run
  concurrently per call. CES rate limiting (HTTP 429) is returned as a query
  error, so space out rule evaluation intervals for many resources.
- **TLS.** The alerting client (`newSafeCESClient`) **verifies TLS
  certificates**, the SDK default, and there is no option to skip verification.
  A custom CES endpoint must present a certificate trusted by the Grafana host,
   with custom endpoint CAs installed in its trust store. Plain HTTP is used only
   by the local integration mock. The legacy dashboard client
  (`GetCESClient`) is unchanged and still skips verification.

## Build and test

Requirements: Go (tested with Go 1.27.3; `go.mod` declares `go 1.19`). No
credentials or network access to Huawei Cloud are needed for the tests. They
use fake CES clients and a local `httptest` endpoint, driven through the real
SDK, with dummy keys.

```sh
go mod download
go vet ./pkg/...
go test ./pkg/...
go test -race ./pkg/...

# Static Linux AMD64 backend (no glibc dependency)
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -trimpath -buildvcs=false -ldflags='-s -w' \
  -o dist/gpx_cloudeye-grafana_linux_amd64 ./pkg
file dist/gpx_cloudeye-grafana_linux_amd64   # "statically linked"
```

`go run github.com/magefile/mage build:linux` also works. The plugin SDK's Mage
build sets `CGO_ENABLED=0` and links statically. The binary runs on glibc- and
musl-based Grafana images; no deployment runtime change is part of this work.

`go.sum` is now tracked and `go.mod` lists indirect requirements (`go mod tidy`).
Upstream ignored `go.sum` and omitted indirect requirements, so the backend did
not build from a clean checkout.

## Frontend toolchain

The frontend was **not** rebuilt. The legacy toolchain cannot be installed from
the public npm registry:

- `typescript@4.9.5-h1.AllScenario.devecostudio.r121` is a vendor-internal
  build (npm 404).
- `@cloud/eslint-config-cbc@1.7.11` is a private package (npm 404).
- `@grafana/toolkit@8.2.1` is deprecated in favour of `@grafana/create-plugin`
  and targets the Node/webpack versions of 2021.

A rebuild would need a migration to `@grafana/create-plugin`. That is out of
scope here, and alerting needs no frontend change.

## Packaging (fork build)

`scripts/package-alerting.py` produces the installable archive. Release builds
must come from a committed, clean tree:

```sh
git status --porcelain        # must be empty for a release build
python3 scripts/package-alerting.py --version 1.1.0-alerting.1
# offline, using a local copy of the pinned upstream archive:
python3 scripts/package-alerting.py --version 1.1.0-alerting.1 \
  --upstream-archive /tmp/opencode/cloudeye-v1.0.3.tar.gz
# local test build from a dirty tree (version becomes 1.1.0-alerting.1-dirty):
python3 scripts/package-alerting.py --version 1.1.0-alerting.1 --allow-dirty \
  --upstream-archive /tmp/opencode/cloudeye-v1.0.3.tar.gz
```

- The script refuses a dirty working tree unless `--allow-dirty` is passed.
  With that flag, `-dirty` is appended to the version. Never release a `-dirty`
  build.
- `info.updated` in `plugin.json` comes from `SOURCE_DATE_EPOCH` if set,
  otherwise from the source commit timestamp, so it is not the build date.
- The backend is built with
  `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -buildvcs=false -ldflags=-s -w`.
  `-mod=readonly` means the committed `go.mod`/`go.sum` must be complete.
- The output goes to `artifacts/` (override with `--output`):
  - `cloudeye-grafana.v<version>.tar.gz`, laid out as
    `huawei-cloudeye-grafana/dist/...`;
  - `cloudeye-grafana.v<version>.tar.gz.sha256`.

**Disclosure. This archive is a fork build, not an official Huawei release:**

- **Frontend:** the compiled frontend (`module.js`, templates, images,
  `metric.yaml`) is the **unchanged upstream v1.0.3 release build**, from
  `https://github.com/huaweicloud/cloudeye-grafana/releases/download/v1.0.3/cloudeye-grafana.v1.0.3.tar.gz`.
  It is verified against the pinned SHA-256
  `218d073722102199aba7e144f2aaa7b05bbcd705d4b651c74c00fb30e166c72e`. No
  frontend source in this fork was compiled into it, because the legacy
  frontend toolchain is unavailable (see above).
- **plugin.json:** only the upstream `plugin.json` is modified, with
  `"alerting": true`, `info.version` and `info.updated`.
- **Backend:** `gpx_cloudeye-grafana_linux_amd64` is built from this fork's
  source as a static Linux AMD64 binary. Linux AMD64 is the only platform built.
- **Plugin ID:** the upstream ID `huawei-cloudeye-grafana` is **kept on
  purpose** for compatibility. Existing datasources, dashboards and provisioning
  keep working, and the fork build replaces the upstream plugin in place. Do not
  install both at the same time.
- **Provenance:** `dist/FORK-BUILD.json` records:
  - the version and source commit;
  - whether the tree was dirty;
  - the Go version and the `sourceDateEpoch` used;
  - the frontend source URL and SHA-256;
  - the exact backend build command.
- **Unsigned:** the plugin is unsigned, as upstream is. Grafana needs
  `allow_loading_unsigned_plugins = huawei-cloudeye-grafana`.

## Grafana 13 compatibility

- **Backend:** `grafana-plugin-sdk-go` v0.166.0 is unchanged; no SDK upgrade is
  part of this work. `QueryData` returns standard data frames
  (`timeseries-multi`), and no frames for No Data.
- **Frontend:** the upstream compiled `module.js` is an AMD module. It imports
  `react`, `lodash`, `@grafana/ui`, `@grafana/data` and `@grafana/runtime`, and
  uses `LegacyForms` (`FormField`, `SecretFormField`), `SegmentAsync`,
  `InlineFormLabel`, `InlineSwitch`, `Select`, `MutableDataFrame`,
  `DataSourceWithBackend` and `getTemplateSrv`. A static check against the
  `@grafana/ui`, `@grafana/data` and `@grafana/runtime` 13.1.10 type
  declarations shows all of these are still exported. Several are deprecated,
  so a future Grafana release may remove them.
- The static check is not a runtime test. Use the Grafana 13 Docker
  integration tests (`tests/integration/`) to verify scheduled alert
  evaluation end to end.
- `grafanaDependency` stays `>=7.0.0`.
