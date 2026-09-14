"""Tick poller of the Lighter-only pair XAU-PERP (m92) vs PAXG-PERP (m48).

Why this exists: minute candles cannot answer "how many seconds does a
>=40 bp dislocation live". This script polls the live order books every
--interval seconds and logs executable (bid/ask) spreads for a fixed
notional (default $1000 ~ 0.23 oz at ~$4324).

Polling vs stream:
  * polling (this script): plain HTTP GET via stdlib urllib, simple and
    robust, no dependencies. Granularity = interval (2-5 s). Two fetches
    (XAU then PAXG) are not simultaneous -- skew ~0.2-0.5 s is recorded.
    Misses sub-second moves.
  * stream (future): persistent wss://.../stream, subscribe ticker/92 and
    ticker/48, tick-by-tick ms updates. Precise durations but needs a
    hand-rolled websocket client (stdlib has none), reconnects, nonce
    continuity. Do polling first, add stream only if polling shows we miss
    episodes.

Read-only public endpoints, no accounts, no trading. DIAGNOSTIC_ONLY.

Usage:
    python3 src/record_tick_lighter.py --once --outdir data
    python3 src/record_tick_lighter.py --interval 2 --threshold 5 --outdir data

Output: data/tick_xau_paxg_YYYY-MM-DD.csv, one row per poll.
Case flag: case_gt = 1 when entry spread > threshold (default 5 bp), so we
can verify the logger sees motion long before a rare 40 bp episode.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

BASE = "https://mainnet.zklighter.elliot.ai/api/v1"

FIELDS = [
    "ts_utc", "xau_bid", "xau_ask", "paxg_bid", "paxg_ask",
    "qty_oz", "notional_usd", "skew_ms",
    "spread_entry_bp", "spread_exit_bp", "roundtrip_bp", "case_gt",
]


def _get_book(mid: int, limit: int = 5, timeout: int = 15) -> dict:
    url = f"{BASE}/orderBookOrders?market_id={mid}&limit={limit}"
    req = urllib.request.Request(url, headers={"User-Agent": "light-arb-spread/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def _best(book: dict) -> tuple[float | None, float | None]:
    try:
        bid = float(book["bids"][0]["price"]) if book.get("bids") else None
        ask = float(book["asks"][0]["price"]) if book.get("asks") else None
    except (KeyError, IndexError, TypeError, ValueError):
        return None, None
    return bid, ask


def poll_once(notional_usd: float, limit: int = 5) -> dict:
    t0 = time.time()
    bx = _get_book(92, limit=limit)
    t1 = time.time()
    bp = _get_book(48, limit=limit)
    t2 = time.time()
    xau_bid, xau_ask = _best(bx)
    paxg_bid, paxg_ask = _best(bp)
    qty = notional_usd / xau_ask if xau_ask else None
    entry = exit_ = rt = None
    if xau_bid and paxg_ask:
        entry = (xau_bid - paxg_ask) / xau_bid * 1e4
    if xau_ask and paxg_bid:
        exit_ = (xau_ask - paxg_bid) / xau_ask * 1e4
    if entry is not None and exit_ is not None:
        rt = entry - exit_
    return {
        "ts_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "xau_bid": xau_bid, "xau_ask": xau_ask,
        "paxg_bid": paxg_bid, "paxg_ask": paxg_ask,
        "qty_oz": round(qty, 4) if qty else "",
        "notional_usd": notional_usd,
        "skew_ms": int((t2 - t1) * 1000) if t1 else "",
        "spread_entry_bp": round(entry, 2) if entry is not None else "",
        "spread_exit_bp": round(exit_, 2) if exit_ is not None else "",
        "roundtrip_bp": round(rt, 2) if rt is not None else "",
        "_fetch_ms": int((t2 - t0) * 1000),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--outdir", default="data")
    ap.add_argument("--interval", type=float, default=2.0)
    ap.add_argument("--threshold", type=float, default=5.0)
    ap.add_argument("--notional", type=float, default=1000.0)
    ap.add_argument("--limit", type=int, default=5)
    ap.add_argument("--max-polls", type=int, default=0)
    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    n = 0
    while True:
        t0 = time.time()
        now = datetime.now(timezone.utc)
        try:
            row = poll_once(args.notional, limit=args.limit)
            entry = row["spread_entry_bp"]
            case = int(entry != "" and abs(float(entry)) > args.threshold)
            row["case_gt"] = case
            fetch_ms = row.pop("_fetch_ms")
            path = os.path.join(args.outdir, f"tick_xau_paxg_{now:%Y-%m-%d}.csv")
            new = not os.path.exists(path)
            with open(path, "a", newline="", encoding="utf-8") as fh:
                w = csv.DictWriter(fh, fieldnames=FIELDS)
                if new:
                    w.writeheader()
                w.writerow({k: row.get(k, "") for k in FIELDS})
            print(f"{row['ts_utc']} entry={entry}bp exit={row['spread_exit_bp']}bp "
                  f"rt={row['roundtrip_bp']}bp case={case} "
                  f"fetch={fetch_ms}ms skew={row['skew_ms']}ms", flush=True)
        except Exception as e:  # noqa: BLE001 - transient API errors, logged
            print(f"{now:%Y-%m-%dT%H:%M:%SZ} ERROR {type(e).__name__}: {e}", flush=True)
        n += 1
        if args.once or (args.max_polls and n >= args.max_polls):
            return
        time.sleep(max(0.5, args.interval - (time.time() - t0)))


if __name__ == "__main__":
    main()
