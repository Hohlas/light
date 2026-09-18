"""Cross-venue funding recorder for the gold basis + majors (explorer.md dirs 2-3).

Polls Lighter's aggregated funding-rates endpoint (one call gives XAU/PAXG/XAG
plus BTC/ETH rates across lighter/binance/bybit/hyperliquid) plus optional
one-shot Binance funding history backfills for retrospective context.

Read-only public endpoints, no accounts, no trading. Status: DIAGNOSTIC_ONLY.

Units: Lighter rows are stored raw as returned (same scale as the existing
`funding` rows in spread_*.csv, labelled per_8h there). Binance history rows
carry their own fundingTime; rate is per funding interval of that symbol.
Do NOT mix units without conversion -- see data/README.md.

Usage:
    python3 src/record_funding.py [--once] [--outdir data]
    python3 src/record_funding.py --backfill-binance [--outdir data]
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

POLL_S = 600
LIGHTER_FUNDING = "https://mainnet.zklighter.elliot.ai/api/v1/funding-rates"
SYMBOLS = ("XAU", "PAXG", "XAG", "BTC", "ETH")

FIELDS = ["ts_utc", "exchange", "symbol", "market_id", "rate", "rate_bp"]


def _get(url: str, timeout: int = 20):
    req = urllib.request.Request(url, data=None)
    req.add_header("User-Agent", "light-arb-spread/1.0")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
    return json.loads(raw.decode("utf-8"))


def poll_lighter() -> list[dict]:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    d = _get(LIGHTER_FUNDING)
    out = []
    for x in d.get("funding_rates", []):
        if x.get("symbol") not in SYMBOLS:
            continue
        try:
            rate = float(x.get("rate"))
        except (TypeError, ValueError):
            continue
        out.append({"ts_utc": now, "exchange": x.get("exchange"),
                    "symbol": x.get("symbol"), "market_id": x.get("market_id"),
                    "rate": rate, "rate_bp": rate * 1e4})
    return out


def divergence(rows: list[dict], symbol: str) -> float | None:
    vals = [r["rate_bp"] for r in rows if r["symbol"] == symbol]
    return max(vals) - min(vals) if len(vals) > 1 else None


def append(outdir: str, rows: list[dict]) -> str:
    now = datetime.now(timezone.utc)
    path = os.path.join(outdir, f"funding_{now:%Y-%m}.csv")
    new = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS)
        if new:
            w.writeheader()
        w.writerows(rows)
    return path


def backfill_binance(outdir: str, symbol: str = "PAXG") -> str:
    pair = f"{symbol}USDT"
    url = (f"https://fapi.binance.com/fapi/v1/fundingRate"
           f"?symbol={pair}&limit=1000")
    hist = _get(url)
    rows = [{"ts_utc": datetime.fromtimestamp(h["fundingTime"] / 1000,
                                              tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
             "exchange": "binance", "symbol": symbol, "market_id": "",
             "rate": float(h["fundingRate"]),
             "rate_bp": float(h["fundingRate"]) * 1e4} for h in hist]
    path = os.path.join(outdir, f"funding_binance_{symbol.lower()}_hist.csv")
    new = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS)
        if new:
            w.writeheader()
        w.writerows(rows)
    return path, len(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--backfill-binance", action="store_true")
    ap.add_argument("--backfill-symbol", default="PAXG")
    ap.add_argument("--outdir", default="data")
    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    if args.backfill_binance:
        try:
            path, n = backfill_binance(args.outdir, args.backfill_symbol.upper())
            print(f"backfill {args.backfill_symbol.upper()} -> {path} rows={n}", flush=True)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError,
                OSError, json.JSONDecodeError, KeyError) as e:
            print(f"backfill ERROR {type(e).__name__}: {e}", flush=True)
        return

    while True:
        now = datetime.now(timezone.utc)
        t0 = time.time()
        try:
            rows = poll_lighter()
            path = append(args.outdir, rows)
            dx = divergence(rows, "XAU")
            dp = divergence(rows, "PAXG")
            db = divergence(rows, "BTC")
            de = divergence(rows, "ETH")
            print(f"{now:%Y-%m-%dT%H:%M:%SZ} rows={len(rows)} "
                  f"divXAU_bp={round(dx, 3) if dx is not None else None} "
                  f"divPAXG_bp={round(dp, 3) if dp is not None else None} "
                  f"divBTC_bp={round(db, 3) if db is not None else None} "
                  f"divETH_bp={round(de, 3) if de is not None else None} "
                  f"-> {path}", flush=True)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError,
                OSError, json.JSONDecodeError, KeyError) as e:
            print(f"{now:%Y-%m-%dT%H:%M:%SZ} ERROR {type(e).__name__}: {e}", flush=True)
        if args.once:
            return
        time.sleep(max(5, POLL_S - (time.time() - t0)))


if __name__ == "__main__":
    main()
