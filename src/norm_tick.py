"""Minute norm calculator for the Lighter XAU/PAXG spread.

Reads heartbeat rows (hb=1, one per minute, time-uniform) from the stream
file(s) and writes rolling norms: trimmed mean (P10-P90) of the entry spread
over 1/3/6/12/24 h windows. Entry signal (elsewhere) is |spread - norm_1h| > X.

Usage:
    python3 src/norm_tick.py --once --outdir data
    python3 src/norm_tick.py --outdir data   # continuous, one row per minute

Output: data/tick_norm_YYYY-MM-DD.csv: ts_utc,norm_1h,norm_3h,norm_6h,
norm_12h,norm_24h,n_hb. Empty value while history < window (warm-up).
Status: DIAGNOSTIC_ONLY, read-only input, append-only output.

NOTE (perf): load_heartbeats re-reads the full tick stream file(s) every
minute; file grows ~50 MB/day. For long runs prefer offset-based tail read
or a compact heartbeat series (not implemented — see audit F11).
"""

from __future__ import annotations

import argparse
import csv
import glob
import os
import time
from datetime import datetime, timedelta, timezone

WINDOWS_H = (1, 3, 6, 12, 24)
FIELDS = ["ts_utc", "norm_1h", "norm_3h", "norm_6h", "norm_12h", "norm_24h", "n_hb"]


def _parse_ts(s: str) -> datetime | None:
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def load_heartbeats(outdir: str, since: datetime) -> list[tuple[datetime, float]]:
    pts: list[tuple[datetime, float]] = []
    for path in sorted(glob.glob(os.path.join(outdir, "tick_stream_xau_paxg*.csv"))):
        try:
            fh = open(path, encoding="utf-8")
        except OSError:
            continue
        with fh:
            r = csv.DictReader(fh)
            if "hb" not in (r.fieldnames or []):
                continue
            for row in r:
                if row.get("hb") != "1":
                    continue
                t = _parse_ts(row.get("ts_utc", ""))
                if t is None or t < since:
                    continue
                try:
                    pts.append((t, float(row["spread_entry_bp"])))
                except (TypeError, ValueError):
                    continue
    return pts


def trimmed_mean(vals: list[float]) -> float | None:
    if not vals:
        return None
    s = sorted(vals)
    k = int(0.1 * len(s))
    core = s[k:len(s) - k] if k else s
    if not core:
        return None
    return sum(core) / len(core)


def compute(outdir: str, now: datetime) -> dict:
    since = now - timedelta(hours=24)
    pts = load_heartbeats(outdir, since)
    row: dict = {"ts_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "n_hb": len(pts)}
    for w in WINDOWS_H:
        cut = now - timedelta(hours=w)
        vals = [v for t, v in pts if t >= cut]
        if len(vals) < w * 60:
            row[f"norm_{w}h"] = ""
            continue
        m = trimmed_mean(vals)
        row[f"norm_{w}h"] = round(m, 2) if m is not None else ""
    return row


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--outdir", default="data")
    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    while True:
        now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        row = compute(args.outdir, now)
        path = os.path.join(args.outdir, f"tick_norm_{now:%Y-%m-%d}.csv")
        new = not os.path.exists(path)
        with open(path, "a", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=FIELDS)
            if new:
                w.writeheader()
            w.writerow(row)
        print(" ".join(f"{k}={row[k]}" for k in FIELDS), flush=True)
        if args.once:
            return
        time.sleep(max(10, 60 - (datetime.now(timezone.utc).second)))


if __name__ == "__main__":
    main()
