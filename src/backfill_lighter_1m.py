"""One-off backfill of Lighter 1-minute candles (native minute history).

Lighter keeps ~2+ weeks of 1m candles; Variational has no history at all, so
only Lighter legs can be backfilled. Run once, then re-run occasionally or rely
on the live recorder for the Variational leg.

Usage:
    python3 src/backfill_lighter_1m.py [--days 7] [--outdir data]

Output: data/lighter_{xau,paxg,xautspot}_1m.csv
Columns: ts_utc,open,high,low,close,vol
Status: DIAGNOSTIC_ONLY.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

BASE = "https://mainnet.zklighter.elliot.ai/api/v1/candles"
MARKETS = {92: "xau", 48: "paxg", 2056: "xautspot"}


def fetch(mid: int, start: int, end: int) -> list[dict]:
    url = (f"{BASE}?market_id={mid}&resolution=1m&count_back=1000"
           f"&start_timestamp={start}&end_timestamp={end}")
    for attempt in range(5):
        try:
            with urllib.request.urlopen(url, timeout=40) as r:
                return json.load(r).get("c", [])
        except Exception as e:  # noqa: BLE001 - transient API errors, retried
            print(f"  retry {attempt + 1}: {type(e).__name__}", flush=True)
            time.sleep(6)
    return []


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--outdir", default="data")
    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    now = int(time.time())
    for mid, name in MARKETS.items():
        rows: dict[int, tuple] = {}
        end = now
        horizon = now - args.days * 86400
        while end > horizon:
            c = fetch(mid, max(horizon, end - 500 * 60), end)
            if not c:
                break
            for x in c:
                rows[x["t"] // 1000] = (x["o"], x["h"], x["l"], x["c"], x.get("v", 0))
            end = min(rows) - 1
            time.sleep(2)
            if len(c) < 400:
                break
        path = os.path.join(args.outdir, f"lighter_{name}_1m.csv")
        with open(path, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["ts_utc", "open", "high", "low", "close", "vol"])
            for t in sorted(rows):
                w.writerow([datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                            *rows[t]])
        lo = datetime.fromtimestamp(min(rows), timezone.utc).isoformat() if rows else "none"
        print(f"{name}: n={len(rows)} from {lo}", flush=True)


if __name__ == "__main__":
    main()
