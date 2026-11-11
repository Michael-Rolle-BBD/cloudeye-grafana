#!/usr/bin/env python3
"""End-to-end test: real Grafana (Docker) + cloudeye-grafana plugin + local mock CES API.

SIMULATION ONLY: every Huawei API call goes to mock_ces.py on 127.0.0.1 with dummy
AK/SK. A pass here is not verification against the live Huawei Cloud Eye API.

Python stdlib only. See README.md in this directory for usage.
"""

import argparse
import base64
import io
import json
import os
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
import traceback
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import mock_ces  # noqa: E402

PLUGIN_ID = "huawei-cloudeye-grafana"
BACKEND = "gpx_cloudeye-grafana_linux_amd64"
DEFAULT_IMAGE = "grafana/grafana:13.3.0-ubuntu"
REPO_ROOT = Path(__file__).resolve().parents[2]

# Fixed, obviously-fake configuration. Nothing here is a real credential or account.
PROJECT_ID = "0123456789abcdef0123456789abcdef"
REGION = "mock-region-1"  # not a Huawei region: multi-region mode cannot resolve a real endpoint
DUMMY_AK = "DUMMYAKCLOUDEYEE2E000"
DUMMY_SK = "dummy-secret-key-not-a-real-credential"
NAMESPACE = "SYS.ECS"
METRIC = "cpu_util"
# Fixed absolute window for /api/ds/query and resource checks: 2026-01-01T00:00:00Z + 30 min.
WINDOW_FROM = 1767225600000
WINDOW_TO = WINDOW_FROM + 30 * 60 * 1000

SERIES = {
    "high": [{"name": "instance_id", "value": "i-e2e-high"}],
    "low": [{"name": "instance_id", "value": "i-e2e-low"}],
    "empty": [{"name": "instance_id", "value": "i-e2e-empty"}],
    "error": [{"name": "instance_id", "value": "i-e2e-error"}],
    "nulls": [{"name": "instance_id", "value": "i-e2e-nulls"}],
    "mixed": [{"name": "instance_id", "value": "i-e2e-mixed"}],
    "alert": [{"name": "instance_id", "value": "i-e2e-alert"}],
}
CHECK_GROUPS = ("infra", "query", "resource", "alerting")


def dimstr(dims):
    return ",".join("%s:%s" % (d["name"], d["value"]) for d in dims)


def key(name):
    return mock_ces.series_key(NAMESPACE, METRIC, SERIES[name])


def _epoch(iso):
    """Grafana RFC3339 (possibly with nanoseconds) -> epoch seconds; 0 for missing/zero time."""
    if not iso or iso.startswith("0001-"):
        return 0
    base = iso.rstrip("Z").split(".")[0]
    return int(datetime.strptime(base, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp())


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def log(msg):
    print("[%s] %s" % (time.strftime("%H:%M:%S"), msg), flush=True)


class CheckFailed(Exception):
    recorded = False


class Deadline:
    def __init__(self, seconds):
        self.end = time.monotonic() + seconds

    def remaining(self):
        return self.end - time.monotonic()

    def check(self, what):
        if self.remaining() <= 0:
            raise CheckFailed("global timeout exceeded while " + what)


# --------------------------------------------------------------------------- plugin staging
def _safe_members(tar, prefixes):
    for member in tar.getmembers():
        if not member.isfile():
            continue
        name = member.name.lstrip("./") if member.name.startswith("./") else member.name
        rel = None
        for prefix in prefixes:
            if name.startswith(prefix):
                rel = name[len(prefix):]
                break
        if rel is None:
            raise RuntimeError("unexpected archive entry outside %s: %s" % (prefixes, member.name))
        p = Path(rel)
        if not rel or p.is_absolute() or ".." in p.parts:
            raise RuntimeError("unsafe archive path: %s" % member.name)
        yield member, p


def stage_from_archive(archive, dest):
    """Extract regular files from <id>/dist/ (or <id>/) into dest; never trust archive paths."""
    with tarfile.open(archive, "r:*") as tar:
        for member, rel in _safe_members(tar, (PLUGIN_ID + "/dist/", PLUGIN_ID + "/")):
            out = dest / rel
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(tar.extractfile(member).read())
            out.chmod(0o755 if rel.name == BACKEND else 0o644)


def stage_dev_build(upstream_archive, dest):
    """Developer fallback: v1.0.3 frontend + backend built from the working tree (go.mod untouched)."""
    stage_from_archive(upstream_archive, dest)
    manifest = json.loads((dest / "plugin.json").read_text())
    manifest["alerting"] = True
    manifest["info"]["version"] = "0.0.0-e2e-dev"
    (dest / "plugin.json").write_text(json.dumps(manifest, indent=2) + "\n")
    log("building backend from working tree (CGO_ENABLED=0, -mod=readonly)")
    subprocess.run(
        ["go", "build", "-mod=readonly", "-trimpath", "-buildvcs=false", "-o", str(dest / BACKEND), "./pkg"],
        cwd=REPO_ROOT, check=True, timeout=600,
        env={**os.environ, "CGO_ENABLED": "0", "GOOS": "linux", "GOARCH": "amd64"},
    )
    (dest / BACKEND).chmod(0o755)


def validate_stage(dest, require_alerting):
    manifest = json.loads((dest / "plugin.json").read_text())
    if manifest.get("id") != PLUGIN_ID:
        raise CheckFailed("plugin.json id is %r" % manifest.get("id"))
    if require_alerting and manifest.get("alerting") is not True:
        raise CheckFailed("plugin.json does not declare alerting:true (use the package-alerting.py output)")
    exe = dest / BACKEND
    if not exe.is_file() or exe.read_bytes()[:4] != b"\x7fELF":
        raise CheckFailed("backend executable %s missing or not ELF" % BACKEND)
    return manifest


def plugin_tar_stream(dest):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for path in sorted(dest.rglob("*")):
            if not path.is_file():
                continue
            info = tar.gettarinfo(str(path), arcname=PLUGIN_ID + "/" + path.relative_to(dest).as_posix())
            info.uid = info.gid = 472
            info.uname = info.gname = "grafana"
            info.mode = 0o755 if path.name == BACKEND else 0o644
            with path.open("rb") as f:
                tar.addfile(info, f)
    return buf.getvalue()


# --------------------------------------------------------------------------- docker / grafana
def docker(*args, input_bytes=None, timeout=120, check=True):
    res = subprocess.run(["docker", *args], input=input_bytes, capture_output=True, timeout=timeout)
    if check and res.returncode != 0:
        raise RuntimeError("docker %s failed: %s" % (args[0], res.stderr.decode(errors="replace").strip()))
    return res


class Grafana:
    def __init__(self, image, port, admin_password):
        self.image, self.port = image, port
        self.base = "http://127.0.0.1:%d" % port
        self.name = "cloudeye-e2e-%s" % secrets.token_hex(4)
        self.auth = "Basic " + base64.b64encode(("admin:" + admin_password).encode()).decode()
        self.admin_password = admin_password
        self.created = False

    def create(self, plugin_tar):
        env = {
            "GF_SERVER_HTTP_ADDR": "127.0.0.1",
            "GF_SERVER_HTTP_PORT": str(self.port),
            "GF_SECURITY_ADMIN_USER": "admin",
            "GF_SECURITY_ADMIN_PASSWORD": self.admin_password,
            "GF_SECURITY_SECRET_KEY": secrets.token_hex(16),
            "GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS": PLUGIN_ID,
            "GF_PLUGINS_PREINSTALL_DISABLED": "true",
            "GF_ANALYTICS_REPORTING_ENABLED": "false",
            "GF_ANALYTICS_CHECK_FOR_UPDATES": "false",
            "GF_ANALYTICS_CHECK_FOR_PLUGIN_UPDATES": "false",
            "GF_NEWS_NEWS_FEED_ENABLED": "false",
            "GF_AUTH_ANONYMOUS_ENABLED": "false",
            "GF_UNIFIED_ALERTING_ENABLED": "true",
            "GF_UNIFIED_ALERTING_MIN_INTERVAL": "10s",
            "GF_UNIFIED_ALERTING_HA_LISTEN_ADDRESS": "127.0.0.1:%d" % free_port(),
            "GF_LOG_MODE": "console",
            "GF_LOG_LEVEL": "info",
        }
        args = ["create", "--name", self.name, "--label", "io.cloudeye.e2e=1",
                "--network", "host", "--user", "472:0",
                "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
                "--memory", "1g", "--pids-limit", "1024"]
        for k, v in env.items():
            args += ["-e", "%s=%s" % (k, v)]
        docker(*args, self.image, timeout=120)
        self.created = True
        docker("cp", "-", "%s:/var/lib/grafana/plugins" % self.name, input_bytes=plugin_tar)

    def start(self):
        docker("start", self.name)

    def logs(self):
        res = docker("logs", self.name, check=False, timeout=60)
        return (res.stdout + res.stderr).decode(errors="replace")

    def remove(self):
        if self.created:
            docker("rm", "-f", "-v", self.name, check=False, timeout=60)
            self.created = False

    def request(self, method, path, body=None, headers=None, timeout=30):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method)
        req.add_header("Authorization", self.auth)
        req.add_header("Accept", "application/json")
        if data is not None:
            req.add_header("Content-Type", "application/json")
        for k, v in (headers or {}).items():
            req.add_header(k, v)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                status = resp.status
        except urllib.error.HTTPError as e:
            with e:
                raw, status = e.read(), e.code
        try:
            return status, json.loads(raw) if raw else None
        except ValueError:
            return status, raw.decode(errors="replace")

    def wait_healthy(self, deadline, timeout=180):
        end = time.monotonic() + timeout
        last = None
        while time.monotonic() < end:
            deadline.check("waiting for Grafana health")
            try:
                status, body = self.request("GET", "/api/health", timeout=5)
                if status == 200 and isinstance(body, dict) and body.get("database") == "ok":
                    return body
                last = (status, body)
            except OSError as e:
                last = repr(e)
            state = docker("inspect", "-f", "{{.State.Status}}", self.name, check=False).stdout.decode().strip()
            if state in ("exited", "dead"):
                raise CheckFailed("Grafana container %s; last health: %s" % (state, last))
            time.sleep(2)
        raise CheckFailed("Grafana not healthy after %ss: %s" % (timeout, last))


# --------------------------------------------------------------------------- frame helpers
def frame_columns(frame):
    """Return (fields schema list, values list) from a Grafana data frame JSON."""
    fields = frame.get("schema", {}).get("fields", [])
    values = frame.get("data", {}).get("values", []) if frame.get("data") else []
    return fields, values


def time_value_pairs(frames):
    """Extract [(times, values, labels)] for each frame with a time and a number field."""
    out = []
    for frame in frames or []:
        fields, values = frame_columns(frame)
        t_idx = next((i for i, f in enumerate(fields) if f.get("type") == "time"), None)
        n_idx = next((i for i, f in enumerate(fields) if f.get("type") == "number"), None)
        if t_idx is None or n_idx is None:
            continue
        times = values[t_idx] if len(values) > t_idx else []
        nums = values[n_idx] if len(values) > n_idx else []
        out.append((times, nums, fields[n_idx].get("labels") or {}, fields[n_idx].get("name")))
    return out


def expected_series(mode, filter_name, period, frm=WINDOW_FROM, to=WINDOW_TO):
    ts = mock_ces.expected_timestamps(frm, to, period)
    return ts, [mock_ces.expected_value(mode, filter_name, t, period) for t in ts]


# --------------------------------------------------------------------------- test runner
class Runner:
    def __init__(self, args):
        self.args = args
        self.results = []
        self.deadline = Deadline(args.timeout)
        self.grafana = None
        self.mock = None
        self.ds_uid = None
        self.ds_name = "CloudEye E2E Mock"

    def record(self, name, ok, detail=""):
        self.results.append({"check": name, "ok": bool(ok), "detail": detail})
        log("%s %s%s" % ("PASS" if ok else "FAIL", name, (": " + detail) if detail else ""))
        return ok

    def warn(self, name, ok, detail=""):
        """Non-fatal observation: reported, never fails the run."""
        self.results.append({"check": name, "ok": True, "warning": not ok, "detail": detail})
        log("%s %s%s" % ("PASS" if ok else "WARN", name, (": " + detail) if detail else ""))

    def expect(self, name, cond, detail=""):
        if not self.record(name, cond, detail):
            err = CheckFailed(name + (": " + detail if detail else ""))
            err.recorded = True
            raise err

    def soft(self, name, fn):
        """Run a check group; record any failure (exactly once) and continue with the next one."""
        try:
            self.deadline.check(name)
            fn()
        except CheckFailed as e:
            if not e.recorded:
                self.record(name, False, str(e))
        except Exception as e:  # noqa: BLE001
            self.record(name, False, "%s: %s" % (type(e).__name__, e))

    def new_requests(self, since_seq, api="batch-query-metric-data"):
        return [r for r in self.mock.recorded(api) if r["seq"] > since_seq]

    def last_seq(self):
        reqs = self.mock.recorded()
        return reqs[-1]["seq"] if reqs else 0

    # ---- infra -------------------------------------------------------------
    def check_infra(self):
        g = self.grafana
        status, body = g.request("GET", "/api/plugins/%s/settings" % PLUGIN_ID)
        self.expect("plugin registered", status == 200 and body.get("id") == PLUGIN_ID,
                    "status=%s type=%s" % (status, (body or {}).get("type") if isinstance(body, dict) else body))
        info_version = body.get("info", {}).get("version")
        self.record("plugin version", True, str(info_version))

        status, body = g.request("POST", "/api/datasources", {
            "name": self.ds_name, "type": PLUGIN_ID, "access": "proxy",
            "jsonData": {"projectId": PROJECT_ID, "cesEndpoint": self.mock.url, "region": REGION,
                         "metaConfEnabled": False},
            "secureJsonData": {"accessKey": DUMMY_AK, "secretKey": DUMMY_SK},
        })
        self.expect("datasource created", status == 200, "status=%s %s" % (status, body))
        self.ds_uid = body["datasource"]["uid"]

        status, body = g.request("GET", "/api/frontend/settings")
        meta = (body.get("datasources", {}).get(self.ds_name, {}) or {}).get("meta", {}) if status == 200 else {}
        self.record("frontend settings expose meta.backend", meta.get("backend") is True, "backend=%r" % meta.get("backend"))
        self.record("frontend settings expose meta.alerting (alert rule datasource picker)",
                    meta.get("alerting") is True, "alerting=%r" % meta.get("alerting"))

        seq = self.last_seq()
        status, body = g.request("GET", "/api/datasources/uid/%s/health" % self.ds_uid, timeout=60)
        self.record("datasource health check OK", status == 200 and (body or {}).get("status") == "OK",
                    "status=%s body=%s" % (status, body))
        alarms = [r for r in self.mock.recorded("alarms") if r["seq"] > seq]
        self.expect("health check reached mock ListAlarms", len(alarms) >= 1, "%d requests" % len(alarms))
        r = alarms[-1]
        self.expect("health check signed with dummy AK/SK (SDK-HMAC-SHA256 verified by mock)",
                    r["auth"]["signature_valid"] and r["auth"]["access_key"] == DUMMY_AK, json.dumps(r["auth"]))
        self.expect("health check uses project id path/header",
                    r["path"] == "/V1.0/%s/alarms" % PROJECT_ID and r["headers"].get("x-project-id") == PROJECT_ID,
                    r["path"])

    # ---- /api/ds/query -----------------------------------------------------
    def ds_query(self, queries, frm=WINDOW_FROM, to=WINDOW_TO):
        body = {"from": str(frm), "to": str(to), "queries": []}
        for q in queries:
            body["queries"].append({
                "refId": q["refId"], "datasource": {"type": PLUGIN_ID, "uid": self.ds_uid},
                "region": REGION, "namespace": NAMESPACE, "dimstr": dimstr(SERIES[q["series"]]),
                "metricName": METRIC, "filter": q.get("filter", "average"), "period": q.get("period", "1"),
                "intervalMs": 60000, "maxDataPoints": 1000,
            })
        return self.grafana.request("POST", "/api/ds/query", body, timeout=60)

    def assert_series(self, name, frames, mode, filter_name, period, dims):
        pairs = time_value_pairs(frames)
        self.expect(name + ": one numeric frame", len(pairs) == 1, "got %d time/number frames" % len(pairs))
        times, values, labels, field_name = pairs[0]
        exp_t, exp_v = expected_series(mode, filter_name, period)
        self.expect(name + ": millisecond timestamps preserved", times == exp_t,
                    "first got=%s want=%s (n=%d/%d)" % (times[:2], exp_t[:2], len(times), len(exp_t)))
        self.expect(name + ": values match mock (%s)" % filter_name, values == exp_v,
                    "first got=%s want=%s" % (values[:3], exp_v[:3]))
        missing = {d["name"]: d["value"] for d in dims if labels.get(d["name"]) != d["value"]}
        self.expect(name + ": dimension labels", not missing, "labels=%s field=%s" % (labels, field_name))
        self.record(name + ": field name/labels (info)", True, "name=%r labels=%s" % (field_name, labels))

    def assert_batch_request(self, name, reqs, filter_name, period, frm, to, series_names):
        self.expect(name + ": backend called BatchListMetricData", len(reqs) >= 1, "%d requests" % len(reqs))
        seen = {}
        for r in reqs:
            b = r["body"]
            self.expect(name + ": request signed with dummy AK/SK", r["auth"]["signature_valid"], r["auth"]["reason"])
            self.expect(name + ": request from/to = Grafana TimeRange (ms)", b["from"] == frm and b["to"] == to,
                        "from=%s to=%s want %s..%s" % (b["from"], b["to"], frm, to))
            self.expect(name + ": request filter/period", b["filter"] == filter_name and b["period"] == period,
                        "filter=%s period=%s" % (b["filter"], b["period"]))
            for m in b["metrics"]:
                seen[mock_ces.series_key(m["namespace"], m["metric_name"], m["dimensions"])] = m
        want = {key(s) for s in series_names}
        self.expect(name + ": request metrics/dimensions parsed from dimstr", want <= set(seen),
                    "sent=%s want=%s" % (sorted(seen), sorted(want)))

    def check_query(self):
        self.mock.set_mode(key("high"), "high")
        self.mock.set_mode(key("low"), "low")
        self.mock.set_mode(key("empty"), "empty")
        self.mock.set_mode(key("error"), "error")
        self.mock.set_mode(key("nulls"), "nulls")
        self.mock.set_mode(key("mixed"), "mixed-null:high")

        def high():
            seq = self.last_seq()
            status, body = self.ds_query([{"refId": "A", "series": "high"}])
            res = (body or {}).get("results", {}).get("A", {}) if isinstance(body, dict) else {}
            self.expect("ds/query high: HTTP 200 without error", status == 200 and not res.get("error"),
                        "status=%s error=%s" % (status, res.get("error")))
            self.assert_batch_request("ds/query high", self.new_requests(seq), "average", "1",
                                      WINDOW_FROM, WINDOW_TO, ["high"])
            self.assert_series("ds/query high", res.get("frames"), "high", "average", "1", SERIES["high"])

        def max300():
            seq = self.last_seq()
            status, body = self.ds_query([{"refId": "A", "series": "low", "filter": "max", "period": "300"}])
            res = (body or {}).get("results", {}).get("A", {})
            self.expect("ds/query max/300: HTTP 200", status == 200 and not res.get("error"), str(res.get("error")))
            self.assert_batch_request("ds/query max/300", self.new_requests(seq), "max", "300",
                                      WINDOW_FROM, WINDOW_TO, ["low"])
            self.assert_series("ds/query max/300", res.get("frames"), "low", "max", "300", SERIES["low"])

        def multi():
            status, body = self.ds_query([{"refId": "A", "series": "high"}, {"refId": "B", "series": "low"}])
            results = (body or {}).get("results", {})
            self.expect("ds/query multi: HTTP 200", status == 200, "status=%s" % status)
            for ref, mode in (("A", "high"), ("B", "low")):
                self.assert_series("ds/query multi ref %s" % ref, results.get(ref, {}).get("frames"), mode,
                                   "average", "1", SERIES[mode])

        def empty():
            status, body = self.ds_query([{"refId": "A", "series": "empty"}])
            res = (body or {}).get("results", {}).get("A", {})
            rows = sum(len(t) for t, _, _, _ in time_value_pairs(res.get("frames")))
            self.expect("ds/query empty: no error and zero rows", status == 200 and not res.get("error") and rows == 0,
                        "status=%s error=%s rows=%d frames=%d" % (status, res.get("error"), rows, len(res.get("frames") or [])))

        def error():
            status, body = self.ds_query([{"refId": "A", "series": "error"}])
            res = (body or {}).get("results", {}).get("A", {}) if isinstance(body, dict) else {}
            err = res.get("error") or ""
            self.expect("ds/query error: per-query error surfaced", bool(err),
                        "status=%s error=%r" % (status, err))
            self.warn("ds/query error: message carries CES error code", "ces.0015" in err, err[:200])
            status, body = self.ds_query([{"refId": "A", "series": "high"}])
            self.expect("ds/query after error: plugin still serving", status == 200, "status=%s" % status)

        def all_null():
            status, body = self.ds_query([{"refId": "A", "series": "nulls"}])
            res = (body or {}).get("results", {}).get("A", {})
            pairs = time_value_pairs(res.get("frames"))
            non_null = [v for _, vals, _, _ in pairs for v in vals if v is not None]
            self.expect("ds/query all-null datapoints: no error and no non-null values",
                        status == 200 and not res.get("error") and not non_null,
                        "status=%s error=%s non_null=%s" % (status, res.get("error"), non_null[:3]))
            shape = []
            for f in res.get("frames") or []:
                cols = (f.get("data") or {}).get("values") or []
                shape.append((len(f.get("schema", {}).get("fields", [])), len(cols[0]) if cols else 0))
            self.record("ds/query all-null: response shape (info)", True,
                        "frames=%d (fields, rows)=%s" % (len(res.get("frames") or []), shape))

        def mixed_null():
            status, body = self.ds_query([{"refId": "A", "series": "mixed"}])
            res = (body or {}).get("results", {}).get("A", {})
            self.expect("ds/query mixed-null: HTTP 200 without error", status == 200 and not res.get("error"),
                        "status=%s error=%s" % (status, res.get("error")))
            pairs = time_value_pairs(res.get("frames"))
            self.expect("ds/query mixed-null: one numeric frame", len(pairs) == 1, "got %d" % len(pairs))
            times, values, labels, _ = pairs[0]
            want = dict(mock_ces.series_points("mixed-null:high", "average", WINDOW_FROM, WINDOW_TO, "1"))
            got = dict(zip(times, values))
            extra = sorted(set(got) - set(want))
            missing = sorted(t for t, v in want.items() if v is not None and t not in got)
            wrong = sorted(t for t, v in got.items() if t in want and v != want[t])
            self.expect("ds/query mixed-null: every non-null point kept with exact ms timestamp and value",
                        not extra and not missing and not wrong,
                        "extra=%s missing=%s wrong=%s" % (extra[:3], missing[:3], wrong[:3]))
            kept_nulls = sum(1 for v in values if v is None)
            self.record("ds/query mixed-null: null handling (info)", True,
                        "%d rows, %d nulls kept as null, %d mock nulls" % (
                            len(values), kept_nulls, sum(1 for v in want.values() if v is None)))
            self.expect("ds/query mixed-null: labels", labels.get("instance_id") == "i-e2e-mixed", str(labels))

        for fn in (high, max300, multi, empty, all_null, mixed_null, error):
            self.soft("ds/query " + fn.__name__, fn)

    # ---- legacy dashboard resource path ------------------------------------
    def check_resource(self):
        self.mock.set_mode(key("high"), "high")

        def resource():
            seq = self.last_seq()
            body = {  # exactly what src/datasource.ts listMetricDataByCustom posts
                "metrics": [{"namespace": NAMESPACE, "dimensions": SERIES["high"], "metric_name": METRIC}],
                "refIDs": ["A"], "from": WINDOW_FROM, "to": WINDOW_TO,
                "filter": "average", "period": "1", "region": REGION,
            }
            status, resp = self.grafana.request("POST", "/api/datasources/uid/%s/resources/metric-data" % self.ds_uid,
                                                body, timeout=60)
            self.expect("resources/metric-data: HTTP 200", status == 200, "status=%s body=%s" % (status, str(resp)[:300]))
            self.assert_batch_request("resources/metric-data", self.new_requests(seq), "average", "1",
                                      WINDOW_FROM, WINDOW_TO, ["high"])
            frames = resp["data"]["results"]["A"]["frames"]
            # The unchanged v1.0.3 frontend reads frames[0].data.values[0] (time) and [1] (value).
            times, values = frames[0]["data"]["values"][0], frames[0]["data"]["values"][1]
            exp_t, exp_v = expected_series("high", "average", "1")
            self.expect("resources/metric-data: frontend-shaped values[0]=time values[1]=value",
                        values == exp_v and len(times) == len(exp_t),
                        "values=%s times=%s" % (values[:2], times[:2]))
            self.warn("resources/metric-data: ms timestamps preserved (legacy path)", times == exp_t,
                        "got=%s want=%s" % (times[:2], exp_t[:2]))
            labels = frames[0]["schema"]["fields"][1].get("labels") or {}
            self.expect("resources/metric-data: labels", labels.get("instance_id") == "i-e2e-high",
                        "labels=%s" % labels)

        def post(series):
            body = {"metrics": [{"namespace": NAMESPACE, "dimensions": SERIES[series], "metric_name": METRIC}],
                    "refIDs": ["A"], "from": WINDOW_FROM, "to": WINDOW_TO,
                    "filter": "average", "period": "1", "region": REGION}
            return self.grafana.request("POST", "/api/datasources/uid/%s/resources/metric-data" % self.ds_uid,
                                        body, timeout=60)

        def frontend_reads(resp):
            """Mimic src/datasource.ts processResponse: frames[0].data.values[0] / [1] must exist."""
            try:
                vals = resp["data"]["results"]["A"]["frames"][0]["data"]["values"]
                return vals[0], vals[1]
            except (KeyError, IndexError, TypeError):
                return None

        def resource_empty():
            self.mock.set_mode(key("empty"), "empty")
            status, resp = post("empty")
            read = frontend_reads(resp) if isinstance(resp, dict) else None
            self.expect("resources/metric-data empty: unchanged frontend can read frames[0] (no TypeError)",
                        status == 200 and read is not None and read == ([], []),
                        "status=%s body=%s" % (status, str(resp)[:300]))

        def resource_nulls():
            self.mock.set_mode(key("mixed"), "mixed-null:high")
            status, resp = post("mixed")
            read = frontend_reads(resp) if isinstance(resp, dict) else None
            self.warn("resources/metric-data mixed-null: usable response (legacy path)",
                      status == 200 and read is not None,
                      "status=%s body=%r" % (status, str(resp)[:300]))
            status, _ = post("high")
            self.expect("resources/metric-data after null data: plugin still serving", status == 200,
                        "status=%s" % status)

        self.soft("resources/metric-data", resource)
        self.soft("resources/metric-data empty", resource_empty)
        self.soft("resources/metric-data nulls", resource_nulls)

    # ---- alerting ----------------------------------------------------------
    def setup_alerting(self):
        g = self.grafana
        hdr = {"X-Disable-Provenance": "true"}
        status, body = g.request("POST", "/api/folders", {"title": "CloudEye E2E", "uid": "cloudeye-e2e"})
        self.expect("alert folder created", status == 200, "status=%s %s" % (status, body))
        cp = {"name": "cloudeye-e2e-mock-webhook", "type": "webhook", "uid": "cloudeye-e2e-webhook",
              "settings": {"url": self.mock.url + "/__mock/webhook", "httpMethod": "POST"},
              "disableResolveMessage": False}
        status, body = g.request("POST", "/api/v1/provisioning/contact-points", cp, headers=hdr)
        self.expect("webhook contact point -> local mock", status in (200, 202), "status=%s %s" % (status, body))
        policy = {"receiver": cp["name"], "group_by": ["grafana_folder", "alertname"],
                  "group_wait": "1s", "group_interval": "5s", "repeat_interval": "1h"}
        status, body = g.request("PUT", "/api/v1/provisioning/policies", policy, headers=hdr)
        self.expect("notification policy -> local mock only", status in (200, 202), "status=%s %s" % (status, body))

        query_model = {
            "refId": "A", "datasource": {"type": PLUGIN_ID, "uid": self.ds_uid},
            "region": REGION, "namespace": NAMESPACE, "dimstr": dimstr(SERIES["alert"]),
            "metricName": METRIC, "filter": "average", "period": "1",
            "intervalMs": 60000, "maxDataPoints": 43200,
        }
        rule = {
            "uid": "cloudeye-e2e-cpu", "title": "CloudEye E2E cpu_util high", "condition": "C",
            "data": [
                {"refId": "A", "datasourceUid": self.ds_uid, "queryType": "",
                 "relativeTimeRange": {"from": self.args.alert_range, "to": 0}, "model": query_model},
                {"refId": "B", "datasourceUid": "__expr__", "relativeTimeRange": {"from": 0, "to": 0},
                 "model": {"refId": "B", "type": "reduce", "expression": "A", "reducer": "last",
                           "datasource": {"type": "__expr__", "uid": "__expr__"}}},
                {"refId": "C", "datasourceUid": "__expr__", "relativeTimeRange": {"from": 0, "to": 0},
                 "model": {"refId": "C", "type": "threshold", "expression": "B",
                           "conditions": [{"evaluator": {"type": "gt", "params": [50]}}],
                           "datasource": {"type": "__expr__", "uid": "__expr__"}}},
            ],
            "noDataState": "NoData", "execErrState": "Error", "for": "%ds" % self.args.alert_for,
            "labels": {"e2e": "cloudeye"}, "annotations": {"summary": "simulated CES data (mock)"},
            "isPaused": False,
        }
        group = {"title": "cloudeye-e2e", "interval": self.args.alert_interval, "rules": [rule]}
        status, body = g.request("PUT", "/api/v1/provisioning/folder/cloudeye-e2e/rule-groups/cloudeye-e2e",
                                 group, headers=hdr)
        self.expect("alert rule group provisioned (interval %ds, for %ds)" % (self.args.alert_interval, self.args.alert_for),
                    status in (200, 202), "status=%s %s" % (status, str(body)[:500]))

    def rule_state(self):
        status, body = self.grafana.request("GET", "/api/prometheus/grafana/api/v1/rules", timeout=30)
        if status != 200:
            return None
        for grp in body.get("data", {}).get("groups", []):
            for rule in grp.get("rules", []):
                if rule.get("uid") == "cloudeye-e2e-cpu" or rule.get("name") == "CloudEye E2E cpu_util high":
                    states = sorted({a.get("state", "") for a in rule.get("alerts") or []})
                    return {"rule_state": rule.get("state"), "health": rule.get("health"),
                            "lastError": rule.get("lastError", ""), "alert_states": states,
                            "lastEvaluation": rule.get("lastEvaluation")}
        return None

    @staticmethod
    def classify(st):
        """Collapse Grafana instance states ("Normal", "Pending (Error)", "NoData", ...) to one label."""
        if not st:
            return "unknown"
        states = [s.strip() for s in st["alert_states"]]
        if any(s.startswith("Error") for s in states):
            return "Error"
        if any(s.startswith("NoData") for s in states):
            return "NoData"
        if any(s.startswith("Alerting") for s in states):
            return "Alerting"
        pending = [s for s in states if s.startswith("Pending")]
        if pending:
            if any("Error" in s for s in pending):
                return "Pending(Error)"
            if any("NoData" in s for s in pending):
                return "Pending(NoData)"
            return "Pending"
        if not states and st["health"] in ("error", "nodata"):
            return {"error": "Error", "nodata": "NoData"}[st["health"]]
        if st["health"] == "ok" and st["rule_state"] == "inactive":
            return "Normal"
        return "unknown"

    def wait_state(self, want, since_seq, timeout, accept_intermediate=()):
        """Wait until the rule is in `want` after the mock served the new mode; return timeline."""
        end = time.monotonic() + timeout
        timeline, last = [], None
        while time.monotonic() < end:
            self.deadline.check("waiting for alert state " + want)
            served = [r for r in self.new_requests(since_seq) if key("alert") in (r.get("series") or {})]
            st = self.rule_state()
            cls = self.classify(st)
            if cls != last:
                timeline.append((round(time.monotonic() - self.t0, 1), cls, st))
                log("  alert state -> %s %s" % (cls, json.dumps(st)))
                last = cls
            evaluated = st and served and _epoch(st.get("lastEvaluation")) >= served[0]["received_at_ms"] // 1000 - 1
            if evaluated and cls == want:
                return timeline, served
            time.sleep(1)
        raise CheckFailed("alert did not reach %s within %ss; timeline=%s; last=%s" % (
            want, timeout, [(t, c) for t, c, _ in timeline], json.dumps(timeline[-1][2] if timeline else None)))

    def check_alerting(self):
        self.mock.set_mode(key("alert"), "low")
        self.setup_alerting()
        self.t0 = time.monotonic()
        step = self.args.alert_interval * 3 + self.args.alert_for + 30

        def transition(name, mode, want, extra=None):
            seq = self.last_seq()
            self.mock.set_mode(key("alert"), mode)
            timeline, served = self.wait_state(want, seq, step)
            states = [c for _, c, _ in timeline]
            self.record("alert %s (mock=%s)" % (name, mode), True, " -> ".join(states))
            if extra:
                extra(timeline, served)
            return timeline

        def normal_initial():
            def range_check(_, served):
                b = served[-1]["body"]
                span = b["to"] - b["from"]
                self.expect("alert query uses rule relativeTimeRange", span == self.args.alert_range * 1000,
                            "to-from=%dms want %dms" % (span, self.args.alert_range * 1000))
                skew = abs(b["to"] - served[-1]["received_at_ms"])
                self.expect("alert query 'to' is evaluation time", skew < 60000, "skew=%dms" % skew)
                self.expect("alert query signed with dummy AK/SK", served[-1]["auth"]["signature_valid"])
            transition("initial Normal", "low", "Normal", range_check)

        def firing():
            def ordered(timeline, _):
                states = [c for _, c, _ in timeline]
                self.expect("alert passed through Pending before Alerting",
                            "Pending" in states and states.index("Pending") < states.index("Alerting"), str(states))
                t_p = next(t for t, c, _ in timeline if c == "Pending")
                t_a = next(t for t, c, _ in timeline if c == "Alerting")
                self.expect("pending period honoured", t_a - t_p >= self.args.alert_for - 2,
                            "pending->alerting %.1fs, for=%ds" % (t_a - t_p, self.args.alert_for))
            transition("Normal -> Pending -> Alerting", "high", "Alerting", ordered)
            self.wait_webhook("firing")

        def resolve():
            transition("Alerting -> Normal", "low", "Normal")
            self.wait_webhook("resolved")

        def nodata():
            transition("Normal -> NoData (empty datapoints)", "empty", "NoData")
            transition("NoData -> Normal", "low", "Normal")

        def nodata_nulls():
            transition("Normal -> NoData (all datapoints null)", "nulls", "NoData")
            transition("NoData(null) -> Normal", "low", "Normal")

        def mixed_null():
            transition("Normal -> Alerting (mixed-null high, newest non-null)", "mixed-null:high", "Alerting")
            transition("Alerting(mixed-null) -> Normal", "low", "Normal")

        def error():
            def last_error(_, served):
                st = self.rule_state() or {}
                self.warn("alert Error carries backend error", "ces.0015" in (st.get("lastError") or ""),
                            (st.get("lastError") or "")[:200])
            transition("Normal -> Error (CES HTTP 500)", "error", "Error", last_error)
            transition("Error -> Normal", "low", "Normal")

        for fn in (normal_initial, firing, resolve, nodata, nodata_nulls, error, mixed_null):
            self.soft("alerting " + fn.__name__, fn)

    def wait_webhook(self, status_wanted, timeout=60):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            for w in self.mock.recorded_webhooks():
                p = w["payload"]
                if p.get("status") == status_wanted and any(
                        a.get("labels", {}).get("alertname") == "CloudEye E2E cpu_util high" for a in p.get("alerts", [])):
                    self.record("webhook notification '%s' delivered to local mock" % status_wanted, True)
                    return
            time.sleep(1)
        self.record("webhook notification '%s' delivered to local mock" % status_wanted, False,
                    "seen=%s" % [w["payload"].get("status") for w in self.mock.recorded_webhooks()])

    # ---- orchestration -----------------------------------------------------
    def run(self):
        args = self.args
        tmp = Path(tempfile.mkdtemp(prefix="cloudeye-e2e-"))
        try:
            stage = tmp / PLUGIN_ID
            stage.mkdir()
            if args.plugin_archive:
                stage_from_archive(args.plugin_archive, stage)
            elif args.plugin_dir:
                shutil.copytree(args.plugin_dir, stage, dirs_exist_ok=True)
            else:
                stage_dev_build(args.upstream_archive, stage)
            manifest = validate_stage(stage, require_alerting=not args.allow_no_alerting)
            log("plugin staged: version=%s alerting=%s" % (manifest["info"].get("version"), manifest.get("alerting")))

            self.mock = mock_ces.MockCES(PROJECT_ID, DUMMY_AK, DUMMY_SK, port=args.mock_port, catalog=[
                (NAMESPACE, METRIC, dims, "%") for dims in SERIES.values()]).start()
            log("mock CES (simulation) on %s" % self.mock.url)

            self.grafana = Grafana(args.image, args.grafana_port or free_port(), secrets.token_urlsafe(18))
            self.grafana.create(plugin_tar_stream(stage))
            self.grafana.start()
            log("grafana container %s on %s" % (self.grafana.name, self.grafana.base))
            health = self.grafana.wait_healthy(self.deadline)
            self.record("grafana healthy", True, "version=%s" % health.get("version"))
            self.record("grafana image", health.get("version", "").startswith(args.expect_version),
                        "want %s* got %s" % (args.expect_version, health.get("version")))

            groups = args.checks
            self.check_infra()
            if "query" in groups:
                self.check_query()
            if "resource" in groups:
                self.check_resource()
            if "alerting" in groups:
                self.soft("alerting setup", self.check_alerting)
        except CheckFailed as e:
            log("aborting: %s" % e)
        except Exception as e:  # noqa: BLE001
            self.record("harness", False, "%s: %s" % (type(e).__name__, e))
            traceback.print_exc()
        finally:
            ok = bool(self.results) and all(r["ok"] for r in self.results)
            self.finish(ok)
            if args.keep and self.grafana and self.grafana.created:
                self.keep_alive()
            self.cleanup(tmp)
        return ok

    def finish(self, ok):
        if self.grafana and self.grafana.created:
            logs = self.grafana.logs()
            interesting = [l for l in logs.splitlines() if "cloudeye" in l.lower() or "level=error" in l]
            log("grafana log lines mentioning cloudeye / level=error (last 40):")
            for line in interesting[-40:]:
                print("    " + line[:400])
        if self.args.artifacts_dir:
            out = Path(self.args.artifacts_dir)
            out.mkdir(parents=True, exist_ok=True)
            (out / "report.json").write_text(json.dumps(self.results, indent=2))
            if self.mock:
                (out / "mock-requests.json").write_text(json.dumps(self.mock.recorded(), indent=2))
                (out / "mock-webhooks.json").write_text(json.dumps(self.mock.recorded_webhooks(), indent=2))
            if self.grafana and self.grafana.created:
                (out / "grafana.log").write_text(self.grafana.logs())
            log("artifacts written to %s" % out.resolve())
        passed = sum(r["ok"] for r in self.results)
        log("SUMMARY: %d/%d checks passed -> %s (SIMULATED CES API, not live Huawei)" % (
            passed, len(self.results), "PASS" if ok else "FAIL"))
        for r in self.results:
            if not r["ok"]:
                print("  FAILED: %s: %s" % (r["check"], r["detail"][:500]))
            elif r.get("warning"):
                print("  WARNING: %s: %s" % (r["check"], r["detail"][:500]))

    def keep_alive(self):
        g = self.grafana
        log("--keep: Grafana UI at %s (user admin, generated password %s); mock CES at %s" % (
            g.base, g.admin_password, self.mock.url))
        log("--keep: everything is removed after %ds or on Ctrl-C" % self.args.keep_seconds)
        try:
            end = time.monotonic() + self.args.keep_seconds
            while time.monotonic() < end:
                time.sleep(1)
        except KeyboardInterrupt:
            pass

    def cleanup(self, tmp):
        if self.grafana:
            self.grafana.remove()
            log("removed container %s" % self.grafana.name)
        if self.mock:
            self.mock.stop()
        shutil.rmtree(tmp, ignore_errors=True)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--plugin-archive", type=Path, help="tar.gz from scripts/package-alerting.py")
    src.add_argument("--plugin-dir", type=Path, help="directory containing plugin.json and %s" % BACKEND)
    src.add_argument("--dev-build", action="store_true",
                     help="build backend from working tree + --upstream-archive frontend (alerting patched in temp copy)")
    p.add_argument("--upstream-archive", type=Path, default=Path("/tmp/opencode/cloudeye-v1.0.3.tar.gz"))
    p.add_argument("--image", default=DEFAULT_IMAGE)
    p.add_argument("--expect-version", default="13.3.0")
    p.add_argument("--checks", default=",".join(CHECK_GROUPS),
                   help="comma list from %s (infra always runs)" % ",".join(CHECK_GROUPS))
    p.add_argument("--grafana-port", type=int, default=0, help="127.0.0.1 port (default: free port)")
    p.add_argument("--mock-port", type=int, default=0, help="127.0.0.1 port (default: free port)")
    p.add_argument("--alert-interval", type=int, default=10, help="rule group interval seconds (min 10)")
    p.add_argument("--alert-for", type=int, default=20, help="pending period seconds")
    p.add_argument("--alert-range", type=int, default=600, help="alert query relative range seconds")
    p.add_argument("--timeout", type=int, default=1200, help="global timeout seconds")
    p.add_argument("--allow-no-alerting", action="store_true", help="accept plugin.json without alerting:true")
    p.add_argument("--artifacts-dir", type=Path, help="write report/mock requests/grafana log here")
    p.add_argument("--keep", action="store_true", help="keep Grafana + mock running for UI inspection")
    p.add_argument("--keep-seconds", type=int, default=1800, help="bounded --keep lifetime")
    args = p.parse_args()
    args.checks = {c.strip() for c in args.checks.split(",") if c.strip()}
    unknown = args.checks - set(CHECK_GROUPS)
    if unknown:
        p.error("unknown checks: %s" % ",".join(sorted(unknown)))
    if args.dev_build and not args.upstream_archive.is_file():
        p.error("--dev-build needs --upstream-archive")
    if shutil.which("docker") is None:
        p.error("docker not found")

    def on_term(signum, _frame):
        raise KeyboardInterrupt("signal %d" % signum)

    signal.signal(signal.SIGTERM, on_term)
    sys.exit(0 if Runner(args).run() else 1)


if __name__ == "__main__":
    main()
