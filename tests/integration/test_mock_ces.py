#!/usr/bin/env python3
"""Offline self-tests for mock_ces.py and harness helpers (no Docker, no network beyond 127.0.0.1).

Run: python3 -m unittest discover -s tests/integration -p 'test_*.py' -v
"""

import hashlib
import hmac
import json
import sys
import unittest
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import e2e_grafana  # noqa: E402
import mock_ces  # noqa: E402

PID, AK, SK = "0123456789abcdef0123456789abcdef", "DUMMYAKTEST", "dummy-sk"
DIMS = [{"name": "instance_id", "value": "i-1"}]
KEY = mock_ces.series_key("SYS.ECS", "cpu_util", DIMS)


def sign(method, path, query, body, ak=AK, sk=SK):
    """Independent re-implementation of the SDK signer for request generation."""
    date = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    headers = {"x-project-id": PID, "x-sdk-date": date, "user-agent": "huaweicloud-usdk-go/3.0"}
    signed = sorted(headers)
    cpath = "/".join(mock_ces._escape(p) for p in path.split("/")) + "/"
    canonical = "\n".join([method, cpath, query, "".join("%s:%s\n" % (h, headers[h]) for h in signed),
                           ";".join(signed), hashlib.sha256(body).hexdigest()])
    sts = "SDK-HMAC-SHA256\n%s\n%s" % (date, hashlib.sha256(canonical.encode()).hexdigest())
    sig = hmac.new(sk.encode(), sts.encode(), hashlib.sha256).hexdigest()
    headers["Authorization"] = "SDK-HMAC-SHA256 Access=%s, SignedHeaders=%s, Signature=%s" % (ak, ";".join(signed), sig)
    return headers


class MockCESTest(unittest.TestCase):
    def setUp(self):
        self.mock = mock_ces.MockCES(PID, AK, SK, catalog=[("SYS.ECS", "cpu_util", DIMS, "%")]).start()

    def tearDown(self):
        self.mock.stop()

    def call(self, method, path, query="", body=None, **kw):
        raw = json.dumps(body).encode() if body is not None else b""
        req = urllib.request.Request(self.mock.url + path + ("?" + query if query else ""), data=raw or None,
                                     method=method, headers=sign(method, path, query, raw, **kw))
        if raw:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                return r.status, json.loads(r.read())
        except urllib.error.HTTPError as e:
            with e:
                return e.code, json.loads(e.read())

    def batch(self, **over):
        body = {"metrics": [{"namespace": "SYS.ECS", "metric_name": "cpu_util", "dimensions": DIMS}],
                "period": "1", "filter": "average", "from": 1767225600000, "to": 1767225600000 + 300000}
        body.update(over)
        return self.call("POST", "/V1.0/%s/batch-query-metric-data" % PID, body=body)

    def test_batch_schema_and_ms_timestamps(self):
        self.mock.set_mode(KEY, "high")
        status, body = self.batch()
        self.assertEqual(status, 200)
        m = body["metrics"][0]
        self.assertEqual(set(m), {"unit", "datapoints", "namespace", "metric_name", "dimensions"})
        self.assertEqual(m["dimensions"], DIMS)
        self.assertEqual(m["datapoints"][0], {"average": 90.0, "timestamp": 1767225600123})
        self.assertTrue(all(set(p) == {"average", "timestamp"} for p in m["datapoints"]))
        self.assertTrue(self.mock.recorded()[0]["auth"]["signature_valid"])

    def test_filter_and_period(self):
        self.mock.set_mode(KEY, "low")
        status, body = self.batch(filter="max", period="300")
        self.assertEqual(status, 200)
        pts = body["metrics"][0]["datapoints"]
        self.assertEqual([p["timestamp"] for p in pts], [1767225600000, 1767225900000])
        self.assertEqual(pts[0]["max"], 15.0)

    def test_empty_and_error_modes(self):
        self.mock.set_mode(KEY, "empty")
        self.assertEqual(self.batch()[1]["metrics"][0]["datapoints"], [])
        self.mock.set_mode(KEY, "error:503")
        status, body = self.batch()
        self.assertEqual(status, 503)
        self.assertEqual(body["error_code"], "ces.0015")

    def test_null_modes(self):
        self.mock.set_mode(KEY, "nulls")
        pts = self.batch()[1]["metrics"][0]["datapoints"]
        self.assertTrue(pts and all(p["average"] is None for p in pts))
        self.mock.set_mode(KEY, "mixed-null:high")
        vals = [p["average"] for p in self.batch()[1]["metrics"][0]["datapoints"]]
        self.assertEqual(vals, [90.0, None, 90.5, None, 90.0])

    def test_validation_rejects_second_timestamps(self):
        status, body = self.batch(**{"from": 1767225600, "to": 1767225900})
        self.assertEqual(status, 400)
        self.assertIn("milliseconds", body["error_msg"])

    def test_bad_signature_rejected(self):
        status, body = self.call("GET", "/V1.0/%s/alarms" % PID, "limit=1", sk="wrong")
        self.assertEqual(status, 401)
        self.assertEqual(body["error_code"], "APIGW.0301")

    def test_list_alarms_and_metrics(self):
        self.assertEqual(self.call("GET", "/V1.0/%s/alarms" % PID, "limit=1")[1]["metric_alarms"], [])
        status, body = self.call("GET", "/V1.0/%s/metrics" % PID, "dim.0=instance_id%2Ci-1&namespace=SYS.ECS")
        self.assertEqual(status, 200)
        self.assertEqual(body["metrics"][0]["metric_name"], "cpu_util")

    def test_wrong_project_rejected(self):
        self.assertEqual(self.call("GET", "/V1.0/otherproject/alarms", "limit=1")[0], 403)


class HelperTest(unittest.TestCase):
    def test_time_value_pairs(self):
        frame = {"schema": {"fields": [{"name": "time", "type": "time"},
                                       {"name": "cpu_util", "type": "number", "labels": {"instance_id": "i"}}]},
                 "data": {"values": [[1, 2], [3.0, 4.0]]}}
        self.assertEqual(e2e_grafana.time_value_pairs([frame]), [([1, 2], [3.0, 4.0], {"instance_id": "i"}, "cpu_util")])

    def test_classify(self):
        c = e2e_grafana.Runner.classify
        self.assertEqual(c({"rule_state": "inactive", "health": "ok", "alert_states": ["Normal"]}), "Normal")
        self.assertEqual(c({"rule_state": "pending", "health": "ok", "alert_states": ["Pending"]}), "Pending")
        self.assertEqual(c({"rule_state": "firing", "health": "ok", "alert_states": ["Alerting"]}), "Alerting")
        self.assertEqual(c({"rule_state": "firing", "health": "nodata", "alert_states": ["NoData"]}), "NoData")
        self.assertEqual(c({"rule_state": "firing", "health": "error", "alert_states": ["Error"]}), "Error")


if __name__ == "__main__":
    unittest.main()
