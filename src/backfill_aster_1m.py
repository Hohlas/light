"""One-off backfill of Aster 1-minute candles (native minute history).

Aster keeps kline history (Binance-style REST, no key for market data).
Paradex klines need auth -> live polling only. Run once, re-run as needed.

Usage:
    python3 src/backfill_aster_1m.py [--days 7] [--outdir data]

Output: data/aster_{xau,paxg,xag}_1m.csv
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

BASE = "https://fapi.asterdex.com/fapi/v1/klines"
SYMS = {"XAUUSDT": "xau", "PAXGUSDT": "paxg", "XAGUSDT": "xag",
        "XPTUSDT": "xpt", "XPDUSDT": "xpd", "CLUSDT": "wti",
        "NATGASUSDT": "natgas"}


def fetch(sym: str, end: int) -> list:
    url = f"{BASE}?symbol={sym}&interval=1m&limit=1000&endTime={end}"
    for attempt in range(5):
        try:
            with urllib.request.urlopen(url, timeout=40) as r:
                return json.load(r)
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

    now = int(time.time() * 1000)
    for sym, name in SYMS.items():
        rows: dict[int, tuple] = {}
        end = now
        horizon = now - args.days * 86400 * 1000
        while end > horizon:
            c = fetch(sym, end)
            if not c:
                break
            for k in c:
                rows[int(k[0]) // 1000] = (k[1], k[2], k[3], k[4], k[5])
            end = min(rows) * 1000 - 1
            time.sleep(1)
            if len(c) < 400:
                break
        path = os.path.join(args.outdir, f"aster_{name}_1m.csv")
        with open(path, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["ts_utc", "open", "high", "low", "close", "vol"])
            for t in sorted(rows):
                w.writerow([datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                            *rows[t]])
        print(f"{name}: n={len(rows)}", flush=True)


if __name__ == "__main__":
    main()
