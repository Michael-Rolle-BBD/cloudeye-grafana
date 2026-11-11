#!/usr/bin/env python3
"""Deterministic local mock of the Huawei Cloud Eye (CES) v1 HTTP API.

SIMULATION ONLY. This server imitates the subset of the CES v1 API that the
cloudeye-grafana backend calls through huaweicloud-sdk-go-v3 v0.1.92:

  POST /V1.0/{project_id}/batch-query-metric-data   (BatchListMetricData)
  GET  /V1.0/{project_id}/alarms                    (ListAlarms, used by CheckHealth)
  GET  /V1.0/{project_id}/metrics                   (ListMetrics, used by query editor metadata)

Response field names follow the SDK models in services/ces/v1/model
(BatchListMetricDataResponse -> BatchMetricData -> DatapointForBatchMetric, etc.).
Results from this mock are NOT verification against the live Huawei API.

Requests are verified like the API gateway would: the SDK-HMAC-SHA256 signature
is recomputed with the configured dummy AK/SK, so a passing run proves the
plugin decrypted the dummy secureJsonData and signed with it.

Control endpoints (not part of the Huawei API) live under /__mock/:
  GET  /__mock/requests          recorded API requests (JSON list)
  POST /__mock/reset             clear recorded requests and webhooks
  POST /__mock/scenario          {"series": {"<key>": "<mode>"}, "default": "<mode>", "health": "<mode>"}
  POST /__mock/webhook           Grafana webhook contact point receiver (recorded)
  GET  /__mock/webhooks          recorded webhook notifications

Series key: "<namespace>|<metric_name>|<dim1>:<v1>,<dim2>:<v2>" with dimensions
sorted by name. Modes:
  "high", "low", "value:<float>"  constant-ish values (see expected_value)
  "empty"                         "datapoints": []
  "nulls"                         every datapoint present with "<filter>": null (synthetic edge case)
  "mixed-null:<high|low|value:x>" odd step indices null, the newest datapoint always non-null
  "error", "error:<http status>"  Huawei-style error body
"""

import argparse
import hashlib
import hmac
import json
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qsl, unquote, urlsplit

HIGH_VALUE = 90.0
LOW_VALUE = 10.0
# Raw ("1") period datapoints carry a non-zero millisecond component, so a backend
# that truncates timestamps to whole seconds is detected.
RAW_MS_OFFSET = 123
RAW_STEP_SECONDS = 60
MAX_DATAPOINTS = 3000
VALID_PERIODS = {"1", "300", "1200", "3600", "14400", "86400"}
VALID_FILTERS = {"average", "max", "min", "sum", "variance"}
PATH_RE = re.compile(r"^/V1\.0/(?P<project>[^/]+)/(?P<api>batch-query-metric-data|alarms|metrics)$")


def series_key(namespace, metric_name, dimensions):
    dims = ",".join(
        "%s:%s" % (d["name"], d["value"]) for d in sorted(dimensions, key=lambda d: d["name"])
    )
    return "%s|%s|%s" % (namespace, metric_name, dims)


def step_seconds(period):
    return RAW_STEP_SECONDS if period == "1" else int(period)


def expected_timestamps(from_ms, to_ms, period):
    """Millisecond timestamps the mock serves for [from_ms, to_ms]."""
    step_ms = step_seconds(period) * 1000
    offset = RAW_MS_OFFSET if period == "1" else 0
    first = ((from_ms - offset) // step_ms) * step_ms + offset
    if first < from_ms:
        first += step_ms
    stamps = list(range(first, to_ms + 1, step_ms))
    return stamps[-MAX_DATAPOINTS:]


def base_value(mode):
    if mode == "high":
        return HIGH_VALUE
    if mode == "low":
        return LOW_VALUE
    if mode.startswith("value:"):
        return float(mode.split(":", 1)[1])
    raise ValueError("mode %r has no value" % mode)


def expected_value(mode, filter_name, timestamp_ms, period):
    """Deterministic value for a datapoint. Small per-step ripple checks value/time pairing."""
    v = base_value(mode) + 0.25 * ((timestamp_ms // (step_seconds(period) * 1000)) % 4)
    return {
        "average": v,
        "max": v + 5.0,
        "min": v - 5.0,
        "sum": v * 10.0,
        "variance": 2.5,
    }[filter_name]


def series_points(mode, filter_name, from_ms, to_ms, period):
    """[(timestamp_ms, value-or-None)] the mock serves for one series; [] for "empty"."""
    if mode == "empty":
        return []
    stamps = expected_timestamps(from_ms, to_ms, period)
    if mode == "nulls":
        return [(ts, None) for ts in stamps]
    if mode.startswith("mixed-null:"):
        inner = mode.split(":", 1)[1]
        step_ms = step_seconds(period) * 1000
        return [(ts, None if (ts // step_ms) % 2 == 1 and i != len(stamps) - 1
                 else expected_value(inner, filter_name, ts, period)) for i, ts in enumerate(stamps)]
    return [(ts, expected_value(mode, filter_name, ts, period)) for ts in stamps]


def huawei_error(status, code, msg):
    return status, {"error_code": code, "error_msg": msg, "request_id": "mock-%d" % int(time.time() * 1000)}


def _escape(s):
    out = []
    for ch in s.encode():
        c = chr(ch)
        if c.isascii() and (c.isalnum() or c in "_-~."):
            out.append(c)
        else:
            out.append("%%%02X" % ch)
    return "".join(out)


def verify_signature(method, raw_path, raw_query, headers, body, ak, sk):
    """Recompute the huaweicloud-sdk-go-v3 SDK-HMAC-SHA256 signature (core/auth/signer).

    Returns (access_key, signed_headers, valid, reason).
    """
    auth = headers.get("authorization", "")
    m = re.match(r"^SDK-HMAC-SHA256 Access=([^,]+), SignedHeaders=([^,]+), Signature=([0-9a-f]+)$", auth)
    if not m:
        return None, None, False, "missing or malformed Authorization header"
    access, signed, signature = m.group(1), m.group(2).split(";"), m.group(3)
    date = headers.get("x-sdk-date", "")
    if not date:
        return access, signed, False, "missing X-Sdk-Date"
    path = "/".join(_escape(unquote(p)) for p in raw_path.split("/"))
    if not path.endswith("/"):
        path += "/"
    pairs = sorted((_escape(k), _escape(v)) for k, v in parse_qsl(raw_query, keep_blank_values=True))
    query = "&".join("%s=%s" % kv for kv in pairs)
    canonical_headers = "".join("%s:%s\n" % (h, headers.get(h, "").strip()) for h in signed)
    content_hash = headers.get("x-sdk-content-sha256") or hashlib.sha256(body).hexdigest()
    canonical = "\n".join([method, path, query, canonical_headers, ";".join(signed), content_hash])
    sts = "SDK-HMAC-SHA256\n%s\n%s" % (date, hashlib.sha256(canonical.encode()).hexdigest())
    want = hmac.new(sk.encode(), sts.encode(), hashlib.sha256).hexdigest()
    if access != ak:
        return access, signed, False, "unexpected access key"
    if not hmac.compare_digest(want, signature):
        return access, signed, False, "signature mismatch"
    return access, signed, True, "ok"


class MockCES:
    def __init__(self, project_id, ak, sk, host="127.0.0.1", port=0, catalog=None):
        self.project_id, self.ak, self.sk = project_id, ak, sk
        self.lock = threading.Lock()
        self.requests = []
        self.webhooks = []
        self.series_modes = {}
        self.default_mode = "low"
        self.health_mode = "ok"
        # ListMetrics catalog: list of (namespace, metric_name, dimensions, unit)
        self.catalog = catalog or []
        mock = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"
            server_version = "mock-ces/1"

            def log_message(self, fmt, *args):  # quiet by default
                pass

            def _send(self, status, payload):
                body = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _handle(self):
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else b""
                status, payload = mock.dispatch(self.command, self.path, self.headers, body)
                self._send(status, payload)

            do_GET = do_POST = do_PUT = do_DELETE = _handle

        self.server = ThreadingHTTPServer((host, port), Handler)
        self.server.daemon_threads = True
        self.thread = None

    @property
    def url(self):
        host, port = self.server.server_address[:2]
        return "http://%s:%d" % (host, port)

    def start(self):
        self.thread = threading.Thread(target=self.server.serve_forever, name="mock-ces", daemon=True)
        self.thread.start()
        return self

    def stop(self):
        self.server.shutdown()
        self.server.server_close()

    # ---- scenario control -------------------------------------------------
    def set_mode(self, key, mode):
        with self.lock:
            self.series_modes[key] = mode

    def mode_for(self, key):
        with self.lock:
            return self.series_modes.get(key, self.default_mode)

    def recorded(self, api=None):
        with self.lock:
            return [r for r in self.requests if api is None or r.get("api") == api]

    def recorded_webhooks(self):
        with self.lock:
            return list(self.webhooks)

    def reset(self):
        with self.lock:
            self.requests.clear()
            self.webhooks.clear()

    # ---- request handling -------------------------------------------------
    def dispatch(self, method, raw_target, headers, body):
        parts = urlsplit(raw_target)
        path, query = parts.path, parts.query
        if path.startswith("/__mock/"):
            return self._control(method, path, body)

        lower = {k.lower(): v for k, v in headers.items()}
        record = {
            "seq": None,
            "received_at_ms": int(time.time() * 1000),
            "method": method,
            "path": path,
            "query": dict(parse_qsl(query, keep_blank_values=True)),
            "headers": {k: lower[k] for k in ("x-project-id", "x-sdk-date", "content-type", "user-agent") if k in lower},
        }
        access, signed, valid, reason = verify_signature(method, path, query, lower, body, self.ak, self.sk)
        record["auth"] = {"access_key": access, "signed_headers": signed, "signature_valid": valid, "reason": reason}
        try:
            record["body"] = json.loads(body) if body else None
        except ValueError:
            record["body"] = body.decode("utf-8", "replace")

        m = PATH_RE.match(path)
        if not m:
            status, payload = huawei_error(404, "APIGW.0101", "The API does not exist or has not been published in the environment")
        elif not valid:
            status, payload = huawei_error(401, "APIGW.0301", "Incorrect IAM authentication information: " + reason)
        elif m.group("project") != self.project_id or lower.get("x-project-id") not in (None, self.project_id):
            status, payload = huawei_error(403, "ces.0050", "project id mismatch")
        else:
            record["api"] = m.group("api")
            status, payload = self._api(m.group("api"), method, record)
        record["status"] = status
        record["response"] = payload
        with self.lock:
            record["seq"] = len(self.requests) + 1
            self.requests.append(record)
        return status, payload

    def _api(self, api, method, record):
        if api == "alarms" and method == "GET":
            if self.health_mode.startswith("error"):
                return self._error_for(self.health_mode)
            return 200, {"metric_alarms": [], "meta_data": {"count": 0, "marker": "", "total": 0}}
        if api == "metrics" and method == "GET":
            return self._list_metrics(record["query"])
        if api == "batch-query-metric-data" and method == "POST":
            return self._batch(record)
        return huawei_error(405, "ces.0001", "method not allowed")

    def _list_metrics(self, query):
        if query.get("start"):
            return 200, {"metrics": [], "meta_data": {"count": 0, "marker": "", "total": 0}}
        want_dims = [v for k, v in sorted(query.items()) if k.startswith("dim.")]
        out = []
        for namespace, metric_name, dims, unit in self.catalog:
            if query.get("namespace") and query["namespace"] != namespace:
                continue
            if query.get("metric_name") and query["metric_name"] != metric_name:
                continue
            have = {"%s,%s" % (d["name"], d["value"]) for d in dims}
            if any(w not in have for w in want_dims):
                continue
            out.append({"namespace": namespace, "metric_name": metric_name, "dimensions": dims, "unit": unit})
        marker = "%s.%s" % (out[-1]["namespace"], out[-1]["metric_name"]) if out else ""
        return 200, {"metrics": out, "meta_data": {"count": len(out), "marker": marker, "total": len(out)}}

    @staticmethod
    def _error_for(mode):
        status = int(mode.split(":", 1)[1]) if ":" in mode else 500
        return huawei_error(status, "ces.0015", "mock injected failure (simulated, status %d)" % status)

    def _batch(self, record):
        body = record["body"]
        if not isinstance(body, dict):
            return huawei_error(400, "ces.0014", "Some content in message body is incorrect: body must be a JSON object")
        problems = []
        metrics = body.get("metrics")
        if not isinstance(metrics, list) or not 1 <= len(metrics) <= 500:
            problems.append("metrics must contain 1..500 items")
            metrics = []
        if body.get("period") not in VALID_PERIODS:
            problems.append("period must be one of %s" % sorted(VALID_PERIODS))
        if body.get("filter") not in VALID_FILTERS:
            problems.append("filter must be one of %s" % sorted(VALID_FILTERS))
        frm, to = body.get("from"), body.get("to")
        if not isinstance(frm, int) or not isinstance(to, int) or frm >= to:
            problems.append("from/to must be integers with from < to")
        elif frm < 10 ** 12 or to < 10 ** 12:
            problems.append("from/to must be epoch milliseconds")
        for item in metrics:
            if not isinstance(item, dict) or not item.get("namespace") or not item.get("metric_name"):
                problems.append("each metric needs namespace and metric_name")
            elif not isinstance(item.get("dimensions"), list) or not 1 <= len(item["dimensions"]) <= 4 or any(
                not isinstance(d, dict) or not d.get("name") or not d.get("value") for d in item["dimensions"]
            ):
                problems.append("each metric needs 1..4 dimensions with name and value")
        if problems:
            record["validation_errors"] = problems
            return huawei_error(400, "ces.0014", "Some content in message body is incorrect: " + "; ".join(problems))

        keys = [series_key(m["namespace"], m["metric_name"], m["dimensions"]) for m in metrics]
        modes = [self.mode_for(k) for k in keys]
        record["series"] = dict(zip(keys, modes))
        for mode in modes:
            if mode.startswith("error"):
                return self._error_for(mode)

        result = []
        for item, mode in zip(metrics, modes):
            points = [{body["filter"]: v, "timestamp": ts}
                      for ts, v in series_points(mode, body["filter"], frm, to, body["period"])]
            result.append({
                "unit": "%",
                "datapoints": points,
                "namespace": item["namespace"],
                "metric_name": item["metric_name"],
                "dimensions": [{"name": d["name"], "value": d["value"]} for d in item["dimensions"]],
            })
        return 200, {"metrics": result}

    def _control(self, method, path, body):
        if path == "/__mock/requests" and method == "GET":
            return 200, self.recorded()
        if path == "/__mock/webhooks" and method == "GET":
            return 200, self.recorded_webhooks()
        if path == "/__mock/reset" and method == "POST":
            self.reset()
            return 200, {"ok": True}
        if path == "/__mock/webhook" and method == "POST":
            try:
                payload = json.loads(body or b"{}")
            except ValueError:
                payload = {"raw": body.decode("utf-8", "replace")}
            with self.lock:
                self.webhooks.append({"received_at_ms": int(time.time() * 1000), "payload": payload})
            return 200, {"ok": True}
        if path == "/__mock/scenario" and method == "POST":
            cfg = json.loads(body or b"{}")
            with self.lock:
                self.series_modes.update(cfg.get("series", {}))
                self.default_mode = cfg.get("default", self.default_mode)
                self.health_mode = cfg.get("health", self.health_mode)
            return 200, {"ok": True}
        return 404, {"error": "unknown control endpoint"}


def main():
    parser = argparse.ArgumentParser(description="Run the mock CES API standalone (simulation only).")
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--project-id", default="0123456789abcdef0123456789abcdef")
    parser.add_argument("--ak", default="DUMMYAKCLOUDEYEE2E000")
    parser.add_argument("--sk", default="dummy-secret-key-not-a-real-credential")
    args = parser.parse_args()
    mock = MockCES(args.project_id, args.ak, args.sk, port=args.port)
    print("mock CES listening on %s (127.0.0.1 only); Ctrl-C to stop" % mock.url, flush=True)
    try:
        mock.server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        mock.server.server_close()


if __name__ == "__main__":
    main()
