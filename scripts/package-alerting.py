#!/usr/bin/env python3
"""Package the unchanged v1.0.3 frontend with this fork's static Linux AMD64 backend."""

import argparse
import datetime
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile
import urllib.request


UPSTREAM_URL = (
    "https://github.com/huaweicloud/cloudeye-grafana/releases/download/"
    "v1.0.3/cloudeye-grafana.v1.0.3.tar.gz"
)
UPSTREAM_SHA256 = "218d073722102199aba7e144f2aaa7b05bbcd705d4b651c74c00fb30e166c72e"
PLUGIN_ID = "huawei-cloudeye-grafana"
BACKEND = "gpx_cloudeye-grafana_linux_amd64"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True, help="SemVer without the v prefix")
    parser.add_argument("--upstream-archive", type=Path, help="Offline copy of the pinned archive")
    parser.add_argument("--output", type=Path, default=Path("artifacts"))
    parser.add_argument("--allow-dirty", action="store_true", help="Local test build only; appends -dirty to the version")
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?", args.version):
        parser.error("--version must be a SemVer version without a v prefix")

    root = Path(__file__).resolve().parents[1]
    source_commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True
    ).strip()
    source_dirty = bool(subprocess.check_output(
        ["git", "status", "--porcelain"], cwd=root, text=True
    ).strip())
    if source_dirty and not args.allow_dirty:
        parser.error("working tree is dirty; commit source before release or use --allow-dirty for local tests")
    if source_dirty:
        args.version += "-dirty"
    epoch = int(os.environ.get("SOURCE_DATE_EPOCH") or subprocess.check_output(
        ["git", "show", "-s", "--format=%ct", "HEAD"], cwd=root, text=True
    ).strip())
    go_version = subprocess.check_output(["go", "version"], cwd=root, text=True).strip()
    archive = args.upstream_archive.read_bytes() if args.upstream_archive else urllib.request.urlopen(
        UPSTREAM_URL, timeout=120
    ).read()
    if hashlib.sha256(archive).hexdigest() != UPSTREAM_SHA256:
        raise RuntimeError("upstream frontend archive SHA-256 mismatch")

    with tempfile.TemporaryDirectory(prefix="cloudeye-package-") as temporary:
        stage = Path(temporary) / PLUGIN_ID / "dist"
        stage.mkdir(parents=True)
        # Extract only ordinary files under the known release prefix. Never trust archive paths.
        prefix = PLUGIN_ID + "/dist/"
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as upstream:
            for member in upstream.getmembers():
                if not member.isfile():
                    continue
                if not member.name.startswith(prefix):
                    raise RuntimeError("unexpected upstream archive layout")
                relative = Path(member.name[len(prefix):])
                if relative.is_absolute() or ".." in relative.parts:
                    raise RuntimeError("unsafe upstream archive path")
                destination = stage / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(upstream.extractfile(member).read())
                destination.chmod(0o644)

        source_manifest = json.loads((root / "src/plugin.json").read_text())
        manifest = json.loads((stage / "plugin.json").read_text())
        assert manifest["id"] == source_manifest["id"] == PLUGIN_ID
        assert source_manifest.get("alerting") is True, "enable alerting only after implementing QueryData"
        manifest["alerting"] = True
        manifest["info"]["version"] = args.version
        manifest["info"]["updated"] = datetime.datetime.fromtimestamp(epoch, datetime.timezone.utc).date().isoformat()
        (stage / "plugin.json").write_text(json.dumps(manifest, indent=2) + "\n")

        subprocess.run(
            ["go", "build", "-mod=readonly", "-trimpath", "-buildvcs=false", "-ldflags=-s -w", "-o", str(stage / BACKEND), "./pkg"],
            cwd=root,
            env={**os.environ, "CGO_ENABLED": "0", "GOOS": "linux", "GOARCH": "amd64"},
            check=True,
        )
        (stage / BACKEND).chmod(0o755)
        (stage / "FORK-BUILD.json").write_text(json.dumps({
            "version": args.version,
            "sourceCommit": source_commit,
            "sourceDirty": source_dirty,
            "goVersion": go_version,
            "sourceDateEpoch": epoch,
            "frontendSource": UPSTREAM_URL,
            "frontendSHA256": UPSTREAM_SHA256,
            "backend": "CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -buildvcs=false -ldflags=-s -w",
        }, indent=2) + "\n")

        args.output.mkdir(parents=True, exist_ok=True)
        output = args.output / ("cloudeye-grafana.v" + args.version + ".tar.gz")
        with output.open("wb") as raw, gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode="w") as result:
                for path in sorted(stage.rglob("*")):
                    if not path.is_file():
                        continue
                    info = result.gettarinfo(str(path), arcname=prefix + path.relative_to(stage).as_posix())
                    info.uid = info.gid = info.mtime = 0
                    info.uname = info.gname = ""
                    with path.open("rb") as content:
                        result.addfile(info, content)
        checksum = hashlib.sha256(output.read_bytes()).hexdigest()
        (args.output / (output.name + ".sha256")).write_text(checksum + "  " + output.name + "\n")
        print(str(output.resolve()))
        print(checksum)


if __name__ == "__main__":
    main()
