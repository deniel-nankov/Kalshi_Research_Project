#!/usr/bin/env python3
"""
Tier 2 observability: turn health_report.json into CloudWatch metrics (+ optional S3 text snapshot).

Plain language:
  - Your daily health check already writes health_report.json.
  - This script reads it and sends numbers to AWS CloudWatch (for charts and alarms).
  - Optionally it uploads that JSON plus dataset_stats output to S3 with a timestamp folder.

Uses the **AWS CLI** (`aws`) — no boto3 in this project (avoids Python 3.9 / urllib3 conflicts).
Install on Ubuntu EC2:  sudo bash infra/aws/install-aws-cli-v2.sh  (or bootstrap.sh); attach an IAM role.

Examples:
  uv run python scripts/publish_tier2_observability.py --dry-run
  uv run python scripts/publish_tier2_observability.py
  S3_TIER2_URI=s3://my-bucket/kalshi/tier2/ uv run python scripts/publish_tier2_observability.py
"""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _load_report(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _check_details(report: dict[str, Any], name: str) -> dict[str, Any]:
    for c in report.get("checks", []):
        if c.get("name") == name:
            return dict(c.get("details") or {})
    return {}


def _check_status(report: dict[str, Any], name: str) -> str | None:
    for c in report.get("checks", []):
        if c.get("name") == name:
            return str(c.get("status") or "")
    return None


def _status_value(status: str | None) -> float:
    if status == "PASS":
        return 1.0
    if status == "WARN":
        return 0.5
    if status == "FAIL":
        return 0.0
    return -1.0


def _metric_data_entries(report: dict[str, Any]) -> list[dict[str, Any]]:
    summary = report.get("summary") or {}
    comp = _check_details(report, "COMPLETENESS")
    ref = _check_details(report, "REFERENTIAL_INTEGRITY")
    uniq = _check_details(report, "UNIQUENESS_MARKETS")

    trades = float(comp.get("trades") or 0)
    markets = float(comp.get("markets") or 0)
    orphans = float(ref.get("orphan_tickers") or 0)
    total_m = float(uniq.get("total") or 0)
    distinct_m = float(uniq.get("distinct") or 0)
    dup_rows = max(0.0, total_m - distinct_m)

    ts = report.get("timestamp")
    report_age_h = float("nan")
    if isinstance(ts, str):
        try:
            t = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if t.tzinfo is None:
                t = t.replace(tzinfo=timezone.utc)
            report_age_h = (datetime.now(timezone.utc) - t).total_seconds() / 3600.0
        except ValueError:
            pass

    env_label = os.environ.get("KALSHI_ENV", "default").strip() or "default"
    dims = [{"Name": "Environment", "Value": env_label}]

    def entry(name: str, value: float, unit: str | None = "Count") -> dict[str, Any]:
        e: dict[str, Any] = {"MetricName": name, "Value": value, "Dimensions": dims}
        if unit is not None:
            e["Unit"] = unit
        return e

    out: list[dict[str, Any]] = [
        entry("HealthPassCount", float(summary.get("pass") or 0)),
        entry("HealthWarnCount", float(summary.get("warn") or 0)),
        entry("HealthFailCount", float(summary.get("fail") or 0)),
        entry("TradeRows", trades),
        entry("MarketRows", markets),
        entry("OrphanTickerCount", orphans),
        entry("DuplicateMarketKeyRows", dup_rows),
        entry("CheckSchemaTrades", _status_value(_check_status(report, "SCHEMA_TRADES"))),
        entry("CheckSchemaMarkets", _status_value(_check_status(report, "SCHEMA_MARKETS"))),
        entry("CheckUniquenessTrades", _status_value(_check_status(report, "UNIQUENESS_TRADES"))),
        entry("CheckUniquenessMarkets", _status_value(_check_status(report, "UNIQUENESS_MARKETS"))),
        entry("CheckReferentialIntegrity", _status_value(_check_status(report, "REFERENTIAL_INTEGRITY"))),
    ]
    if not math.isnan(report_age_h):
        out.append(entry("HealthReportAgeHours", report_age_h, unit=None))
    return out


def _aws_bin() -> str:
    return os.environ.get("AWS_CLI_BIN", "aws")


def _run_aws(args: list[str], *, dry_run: bool) -> None:
    exe = _aws_cli_resolved() or _aws_bin()
    cmd = [exe, *args]
    if dry_run:
        print("[dry-run]", " ".join(cmd))
        return
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or f"exit {proc.returncode}")


def _aws_cli_resolved() -> str | None:
    b = _aws_bin()
    p = Path(b)
    if p.is_file():
        return str(p.resolve())
    return shutil.which(p.name)


def _metric_shorthand(m: dict[str, Any]) -> str:
    """CLI shorthand for one MetricDatum (avoids file:// paths that break on Windows)."""
    parts = [f"MetricName={m['MetricName']}", f"Value={m['Value']}"]
    if m.get("Unit") is not None:
        parts.append(f"Unit={m['Unit']}")
    dims = m.get("Dimensions") or []
    if dims:
        inner = ",".join(f"{{Name={d['Name']},Value={d['Value']}}}" for d in dims)
        parts.append(f"Dimensions=[{inner}]")
    return ",".join(parts)


def _put_metric_data_cli(namespace: str, metric_data: list[dict[str, Any]], dry_run: bool) -> None:
    resolved = _aws_cli_resolved()
    if not dry_run and not resolved:
        raise RuntimeError(
            f"AWS CLI not found ({_aws_bin()}). On EC2: sudo bash infra/aws/install-aws-cli-v2.sh "
            "and attach an IAM instance profile (or configure credentials)."
        )
    batch_size = 20
    for i in range(0, len(metric_data), batch_size):
        chunk = metric_data[i : i + batch_size]
        args = ["cloudwatch", "put-metric-data", "--namespace", namespace]
        for m in chunk:
            args.extend(["--metric-data", _metric_shorthand(m)])
        _run_aws(args, dry_run=dry_run)


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    u = uri.strip()
    if not u.startswith("s3://"):
        raise ValueError("S3_TIER2_URI must start with s3://")
    rest = u[5:]
    if "/" not in rest:
        bucket, prefix = rest, ""
    else:
        bucket, prefix = rest.split("/", 1)
    prefix = prefix.strip("/")
    if prefix:
        prefix = prefix + "/"
    return bucket, prefix


def _run_data_stats() -> str:
    proc = subprocess.run(
        [os.environ.get("UV_BIN", "uv"), "run", "python", "scripts/data_stats.py"],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        timeout=3600,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"data_stats.py failed ({proc.returncode}): {proc.stderr[:2000]}")
    return proc.stdout or ""


def _upload_s3_cli(bucket: str, key: str, body: bytes, content_type: str, dry_run: bool) -> None:
    with tempfile.NamedTemporaryFile("wb", suffix=".bin", delete=False) as f:
        f.write(body)
        tmp = f.name
    try:
        local_path = str(Path(tmp).resolve())
        args = [
            "s3",
            "cp",
            local_path,
            f"s3://{bucket}/{key}",
            "--content-type",
            content_type,
        ]
        _run_aws(args, dry_run=dry_run)
    finally:
        Path(tmp).unlink(missing_ok=True)


def main() -> int:
    p = argparse.ArgumentParser(description="Publish Tier 2 observability (CloudWatch + optional S3)")
    p.add_argument(
        "--health-report",
        type=Path,
        default=PROJECT_ROOT / "data/kalshi/state/health_report.json",
        help="Path to health_report.json from validate_data_health.py",
    )
    p.add_argument(
        "--namespace",
        default=os.environ.get("CLOUDWATCH_NAMESPACE", "KalshiData"),
        help="CloudWatch metric namespace (default env CLOUDWATCH_NAMESPACE or KalshiData)",
    )
    p.add_argument("--dry-run", action="store_true", help="Print aws commands only; no uploads")
    p.add_argument(
        "--skip-data-stats",
        action="store_true",
        help="Do not run data_stats.py (still uploads health JSON if S3 is set)",
    )
    args = p.parse_args()

    path: Path = args.health_report
    if not path.is_file():
        print(f"No health report at {path} — run validate_data_health.py first.", file=sys.stderr)
        return 1

    report = _load_report(path)
    namespace = str(args.namespace).strip() or "KalshiData"
    metric_data = _metric_data_entries(report)
    s3_uri = os.environ.get("S3_TIER2_URI", "").strip()

    if args.dry_run:
        print(f"[dry-run] Would send {len(metric_data)} metrics to CloudWatch namespace {namespace!r}")
        for e in metric_data:
            u = e.get("Unit", "")
            print(f"    {e['MetricName']}={e['Value']}" + (f" {u}" if u else ""))
        try:
            _put_metric_data_cli(namespace, metric_data, dry_run=True)
        except RuntimeError as e:
            print(f"Note: {e}", file=sys.stderr)
        if not s3_uri:
            print("S3_TIER2_URI not set - would skip S3 snapshot.")
        else:
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            b, pref = _parse_s3_uri(s3_uri)
            print(f"[dry-run] Would aws s3 cp … s3://{b}/{pref}{stamp}/health_report.json (+ dataset_stats.txt)")
            if os.environ.get("UPLOAD_OPS_SNAPSHOT", "").strip() in ("1", "true", "yes"):
                print("[dry-run] Would also upload ops_snapshot.json if OPS_SNAPSHOT_PATH file exists")
        return 0

    try:
        _put_metric_data_cli(namespace, metric_data, dry_run=False)
        print(f"CloudWatch: put-metric-data OK ({len(metric_data)} metrics) Namespace={namespace!r}")
    except RuntimeError as e:
        print(f"CloudWatch error: {e}", file=sys.stderr)
        return 1

    if not s3_uri:
        print("S3_TIER2_URI not set - skipping S3 snapshot (optional).")
        return 0

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    health_bytes = path.read_bytes()
    try:
        stats_text = "" if args.skip_data_stats else _run_data_stats()
        bucket, key_prefix = _parse_s3_uri(s3_uri)
        base = f"{key_prefix}{stamp}/"
        _upload_s3_cli(
            bucket,
            f"{base}health_report.json",
            health_bytes,
            "application/json",
            dry_run=False,
        )
        _upload_s3_cli(
            bucket,
            f"{base}dataset_stats.txt",
            stats_text.encode("utf-8"),
            "text/plain; charset=utf-8",
            dry_run=False,
        )
        print(f"S3: s3://{bucket}/{base}health_report.json (+ dataset_stats.txt)")
        snap_path = Path(
            os.environ.get(
                "OPS_SNAPSHOT_PATH",
                str(PROJECT_ROOT / "data/kalshi/state/ops_snapshot.json"),
            )
        )
        if os.environ.get("UPLOAD_OPS_SNAPSHOT", "").strip() in ("1", "true", "yes") and snap_path.is_file():
            _upload_s3_cli(
                bucket,
                f"{base}ops_snapshot.json",
                snap_path.read_bytes(),
                "application/json",
                dry_run=False,
            )
            print(f"S3: s3://{bucket}/{base}ops_snapshot.json (ops console / AWS visibility)")
    except (RuntimeError, ValueError) as e:
        print(f"S3 error: {e}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
