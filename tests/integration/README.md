# Integration tests: Grafana 13 + cloudeye-grafana + mock CES API

> **Simulation only.** Every Huawei API call in these tests goes to `mock_ces.py`
> on `127.0.0.1` with dummy AK/SK. A passing run proves the plugin works inside a
> real Grafana `13.3.0-ubuntu` against a CES-shaped API. It is **not** verification
> against the live Huawei Cloud Eye API. No real credentials, accounts, clusters or
> notification endpoints are used.

## Files

| File | Purpose |
| --- | --- |
| `mock_ces.py` | Deterministic mock of CES v1 (`batch-query-metric-data`, `alarms`, `metrics`) with request recording, SDK signature verification and scenario modes. Can also run standalone. |
| `e2e_grafana.py` | Harness: starts the mock, runs Grafana in Docker with the plugin, provisions a datasource and an alert rule, and runs the checks. |
| `test_mock_ces.py` | Offline unit tests for the mock and harness helpers (no Docker). |

Python 3 stdlib only; requires Docker and the `grafana/grafana:13.3.0-ubuntu` image
(pulled automatically on first run).

## Run

```sh
# 1. Offline self-tests for the mock (seconds, no Docker)
python3 -m unittest discover -s tests/integration -p 'test_*.py' -v

# 2a. Release build: clean, committed working tree only
python3 scripts/package-alerting.py --version 1.1.0-alerting.1 \
  --upstream-archive /tmp/opencode/cloudeye-v1.0.3.tar.gz
#     -> artifacts/cloudeye-grafana.v1.1.0-alerting.1.tar.gz

# 2b. Test build of uncommitted backend changes (version gets a -dirty suffix)
python3 scripts/package-alerting.py --version 1.1.0-alerting.1 --allow-dirty \
  --upstream-archive /tmp/opencode/cloudeye-v1.0.3.tar.gz
#     -> artifacts/cloudeye-grafana.v1.1.0-alerting.1-dirty.tar.gz

# 3. Full end-to-end run against a packaged artifact (~7 min).
#    Use the clean archive for release evidence; the -dirty one is for iteration only.
python3 tests/integration/e2e_grafana.py \
  --plugin-archive artifacts/cloudeye-grafana.v1.1.0-alerting.1.tar.gz \
  --artifacts-dir /tmp/cloudeye-e2e-artifacts
```

`scripts/package-alerting.py` belongs to the backend work. It records `sourceCommit` and
`sourceDirty` in `FORK-BUILD.json` inside the archive. The harness logs the plugin version,
so a `-dirty` build is visible in the output.

Exit code is 0 only if all checks pass. The summary lists failed checks, plus non-fatal
warnings. Example warning: the legacy `resources/metric-data` path truncates ms timestamps.

Other plugin sources:

- `--plugin-dir DIR`: a directory with `plugin.json` and `gpx_cloudeye-grafana_linux_amd64`.
- `--dev-build`: developer shortcut. Builds the backend from the working tree
  (`CGO_ENABLED=0 go build -mod=readonly ./pkg`, `go.mod` is not changed) and pairs it
  with the v1.0.3 frontend from `--upstream-archive`. `alerting:true` is patched only in
  a temporary copy. Use the packaged artifact for release evidence.

Useful options: `--checks infra,query,resource,alerting` (infra always runs),
`--timeout` (global, default 1200 s), `--alert-interval`/`--alert-for`/`--alert-range`,
`--image`, `--grafana-port`/`--mock-port` (default: free ports).

### Inspect the Grafana UI

```sh
python3 tests/integration/e2e_grafana.py --plugin-archive ARCHIVE --keep --keep-seconds 1800
```

After the checks, the harness prints the Grafana URL (`127.0.0.1` only) and a generated
admin password, and keeps Grafana and the mock running. On Ctrl-C, or when
`--keep-seconds` expires, it removes everything. Without `--keep` the run cleans up as
soon as the checks finish.

## What is tested

**Infra**
- Grafana 13.3.0 is healthy and registers the unsigned plugin, with the unsigned load allowed only for `huawei-cloudeye-grafana`.
- `/api/frontend/settings` exposes the datasource with `meta.backend=true` and `meta.alerting=true`. This flag is what makes the datasource appear in the alert rule editor.
- The datasource health check reaches mock `ListAlarms`. The mock recomputes the SDK-HMAC-SHA256 signature with the dummy AK/SK. A pass proves Grafana decrypted `secureJsonData` and the SDK signed the request with it. The project ID must appear in both the path and the `X-Project-Id` header.

**`/api/ds/query` (server-side `QueryData`)**. Fixed window: 2026-01-01T00:00Z, 30 min.
- `high` series, `average`, raw period `1`:
  - The backend request's `from`/`to` equal the Grafana TimeRange in ms.
  - `filter`, `period` and the dimensions parsed from `dimstr` are correct.
  - Returned timestamps match the mock **to the millisecond**. Raw datapoints carry a `.123` ms offset, so a backend that truncates to whole seconds fails.
  - Values and dimension labels match.
- `max` with period `300` → aligned timestamps, `max` values.
- Two refIds in one request → each refId gets its own series.
- Empty datapoints → no error and zero rows.
- All-null datapoints (each one present with `"<filter>": null`) → no error and no non-null values.
- Mixed-null datapoints → every non-null point is kept with its exact ms timestamp and value, and no extra timestamps appear. Whether nulls are kept as null or dropped is reported as info.
- CES HTTP 500 → a per-refId error. After that, the plugin still serves queries.

**`/api/datasources/uid/<uid>/resources/metric-data`** (the existing dashboard path used
by the unchanged v1.0.3 frontend)
- Posts exactly the body `src/datasource.ts` builds.
- Checks `data.results.A.frames[0].data.values[0]` (time) and `values[1]` (value), plus field labels.

**Grafana-managed alerting** (real scheduler: rule interval 10 s, pending period 20 s, query range 600 s)
- Threshold: `last(A) > 50`, `noDataState=NoData`, `execErrState=Error`. The datasource query model is the `MyQuery` JSON.
- The mock series is switched through each state:

  | Mock series | Expected alert state |
  | --- | --- |
  | `low` | Normal |
  | `high` | Pending, then Alerting after at least the pending period |
  | `low` | Normal |
  | `empty` | NoData |
  | `low` | Normal |
  | `nulls` (all datapoints null) | NoData. **Must not be Normal.** |
  | `low` | Normal |
  | `error` (HTTP 500) | Pending(Error), then Error |
  | `low` | Normal |
  | `mixed-null:high` (newest datapoint non-null) | Alerting |
  | `low` | Normal |

- Alert queries: `to - from` equals the rule's relative range, `to` is the evaluation time, and requests are signed.
- Notifications go **only** to a webhook contact point on the local mock (`/__mock/webhook`). The root notification policy is replaced, so nothing is sent externally. Firing and resolved webhooks are asserted.

## Mock API contract

The mock follows `huaweicloud-sdk-go-v3@v0.1.92` `services/ces/v1`:
- Paths: `POST /V1.0/{project_id}/batch-query-metric-data`, `GET /V1.0/{project_id}/alarms`, `GET /V1.0/{project_id}/metrics`.
- Response JSON: `{"metrics":[{"unit","datapoints":[{"<filter>":v,"timestamp":ms}],"namespace","metric_name","dimensions":[{"name","value"}]}]}`. As with real CES, only the requested filter field is present in each datapoint.
- Errors: `{"error_code","error_msg","request_id"}`, which the SDK parses into `ServiceResponseError`.
- Request validation: periods `1|300|1200|3600|14400|86400`, filters `average|max|min|sum|variance`, 1–500 metrics with 1–4 dimensions, epoch-**millisecond** `from < to`.

During development, the mock was cross-checked with a throwaway Go program that uses the real SDK client. That program is not committed. It covered:
- Signed `ListAlarms`, `ListMetrics` and `BatchListMetricData` requests, all verified.
- A wrong SK, rejected with 401.
- Responses that decoded with ms timestamps intact.
- Injected errors, which surfaced as `ServiceResponseError`.

Scenario modes: `high`, `low`, `value:<x>`, `empty`, `nulls`, `mixed-null:<mode>`, `error[:<status>]`.
`nulls` is a synthetic edge case. The SDK decodes JSON `null` and a missing filter field
the same way (a nil `*float64`).

Values are deterministic. The base value is `high`=90 or `low`=10, plus `0.25 * (step index mod 4)`. `max` adds 5, `min` subtracts 5, `sum` multiplies by 10, and `variance` is 2.5. Raw period uses 60 s steps at `+123 ms`; aggregated periods are aligned.

Standalone mock for manual work: `python3 tests/integration/mock_ces.py --port 18080`.
Control endpoints: `GET /__mock/requests`, `POST /__mock/scenario`, `POST /__mock/reset`.

## Safety properties

- Grafana runs with `--network host`, but `GF_SERVER_HTTP_ADDR=127.0.0.1`, on a free port. The mock binds `127.0.0.1` only.
- Plugin files are streamed in with `docker cp -`. Snap-packaged Docker cannot see `/tmp` bind mounts, and the stream also avoids host path mounts entirely.
- The container runs with:
  - uid 472
  - `--cap-drop ALL` and `no-new-privileges`
  - 1 GiB memory and a pids limit
  - analytics, update checks, news and plugin preinstall disabled
- The admin password and `secret_key` are generated per run. No local Grafana config or secrets are read.
- The datasource always sets `projectId` and `cesEndpoint`, which selects the SDK's single-endpoint mode. The region `mock-region-1` is not a Huawei region, so a misconfiguration cannot silently resolve to a real endpoint. The Huawei SDK ignores `HTTP(S)_PROXY`, so a proxy env is not a usable egress guard.
- Every external call is bounded:
  - Docker: 120 s
  - HTTP: ≤ 60 s
  - each alert transition: ≤ `3*interval + for + 30` s
  - the whole run: `--timeout`
- Cleanup runs in `finally`: the container is removed (`docker rm -f -v`), the temp dir is deleted and the mock is stopped. SIGTERM is handled. Leftover containers carry the label `io.cloudeye.e2e=1`:
  `docker rm -f $(docker ps -aq --filter label=io.cloudeye.e2e=1)`.
