"""CEX book poller for cross-venue gold basis (context §6 p.16, 18).

Polls public top-of-book + short depth every POLL_S for:
  * Binance PAXGUSDT perp  (bookTicker + depth5)
  * Binance XAUTUSDT spot  (bookTicker)
  * Bybit   PAXGUSDT perp  (orderbook L5)

Cross-venue spreads vs Lighter/Aster are computed in analysis by joining on
ts_utc -- this recorder stays independent and stores raw legs only.

Read-only public endpoints, no accounts, no trading. Status: DIAGNOSTIC_ONLY.

Usage:
    python3 src/poll_cex_books.py [--once] [--outdir data]
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

POLL_S = 60
UA = {"User-Agent": "light-arb-spread/1.0"}
BIN = "https://fapi.binance.com/fapi/v1"
BIN_SPOT = "https://api.binance.com/api/v3"
BYBIT = "https://api.bybit.com/v5/market"

FIELDS = ["ts_utc", "venue", "market", "symbol", "bid", "ask", "bid_qty",
          "ask_qty", "bid_not5_usd", "ask_not5_usd", "spread_bp", "src_ts"]


def _get(url: str, timeout: int = 15):
    req = urllib.request.Request(url, data=None, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def row(venue, market, symbol, bid="", ask="", bid_qty="", ask_qty="",
        bid_not5="", ask_not5="", src_ts=""):
    try:
        sp = (float(ask) - float(bid)) / float(bid) * 1e4
    except (TypeError, ValueError, ZeroDivisionError):
        sp = ""
    return {"ts_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "venue": venue, "market": market, "symbol": symbol, "bid": bid,
            "ask": ask, "bid_qty": bid_qty, "ask_qty": ask_qty,
            "bid_not5_usd": bid_not5, "ask_not5_usd": ask_not5,
            "spread_bp": round(sp, 3) if sp != "" else "", "src_ts": src_ts}


def _notional(levels) -> float:
    tot = 0.0
    for p, q in levels:
        try:
            tot += float(p) * float(q)
        except (TypeError, ValueError):
            pass
    return tot


def poll() -> list[dict]:
    out = []
    b = _get(f"{BIN}/ticker/bookTicker?symbol=PAXGUSDT")
    d = _get(f"{BIN}/depth?symbol=PAXGUSDT&limit=5")
    out.append(row("binance", "perp", "PAXGUSDT", bid=b.get("bidPrice"),
                   ask=b.get("askPrice"), bid_qty=b.get("bidQty"),
                   ask_qty=b.get("askQty"),
                   bid_not5=round(_notional(d.get("bids", [])), 1),
                   ask_not5=round(_notional(d.get("asks", [])), 1),
                   src_ts=b.get("time")))
    s = _get(f"{BIN_SPOT}/ticker/bookTicker?symbol=XAUTUSDT")
    out.append(row("binance", "spot", "XAUTUSDT", bid=s.get("bidPrice"),
                   ask=s.get("askPrice"), bid_qty=s.get("bidQty"),
                   ask_qty=s.get("askQty"), src_ts=s.get("time", "")))
    o = _get(f"{BYBIT}/orderbook?category=linear&symbol=PAXGUSDT&limit=5")
    res = (o.get("result") or {})
    bids, asks = res.get("b", []), res.get("a", [])
    out.append(row("bybit", "perp", "PAXGUSDT",
                   bid=bids[0][0] if bids else "",
                   ask=asks[0][0] if asks else "",
                   bid_qty=bids[0][1] if bids else "",
                   ask_qty=asks[0][1] if asks else "",
                   bid_not5=round(_notional(bids), 1),
                   ask_not5=round(_notional(asks), 1),
                   src_ts=res.get("ts", "")))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--outdir", default="data")
    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    while True:
        now = datetime.now(timezone.utc)
        t0 = time.time()
        try:
            rows = poll()
            path = os.path.join(args.outdir, f"cex_books_{now:%Y-%m-%d}.csv")
            new = not os.path.exists(path)
            with open(path, "a", newline="", encoding="utf-8") as fh:
                w = csv.DictWriter(fh, fieldnames=FIELDS)
                if new:
                    w.writeheader()
                w.writerows(rows)
            desc = " ".join(f"{r['venue']}/{r['market']}={r['spread_bp']}" for r in rows)
            print(f"{now:%Y-%m-%dT%H:%M:%SZ} rows={len(rows)} {desc}", flush=True)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError,
                OSError, json.JSONDecodeError, KeyError, IndexError) as e:
            print(f"{now:%Y-%m-%dT%H:%M:%SZ} ERROR {type(e).__name__}: {e}", flush=True)
        if args.once:
            return
        time.sleep(max(5, POLL_S - (time.time() - t0)))


if __name__ == "__main__":
    main()
