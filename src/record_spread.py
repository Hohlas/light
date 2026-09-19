"""Minute recorder of the XAUT<->XAU basis on the two traded venues only.

Venues: Variational (XAUT-PERP, XAU-PERP) and Lighter (XAU-PERP m92,
PAXG-PERP m48, XAUT/USDC spot m2056, funding).

Read-only public endpoints, no accounts, no trading. Status: DIAGNOSTIC_ONLY.

Design (see docs/xaut-xau-basis-context.md §8):
  * Lighter minute history is re-fetchable from its candles API, so for Lighter
    we only need live snapshots (executable quotes, funding);
  * Variational has NO history endpoint, so its snapshots are polled every
    MINUTE_S and later aggregated into minute bars -- this is the only
    irreplaceable leg;
  * CEX polls (Binance/Bybit/...) were used only for the retrospective
    reconstruction and are NOT recorded: decisions use only traded venues.
  * alert flags: in_window (weekend Fri 20:00 -> Mon 04:00 UTC, daily 11-16 UTC)
    and trigger (|spread| >= TRIGGER_BP on any tracked pair).

Usage:
    python3 src/record_spread.py [--once] [--outdir data]
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

MINUTE_S = 60
TRIGGER_BP = 50.0

VAR_STATS = "https://omni-client-api.prod.ap-northeast-1.variational.io/metadata/stats"
LIGHTER = "https://mainnet.zklighter.elliot.ai/api/v1"

FIELDS = [
    "ts_utc", "venue", "kind", "symbol", "mark", "index", "last",
    "bid", "ask", "funding", "funding_unit", "oi", "src_ts",
]


def _get(url: str, timeout: int = 20, compress: bool = False):
    req = urllib.request.Request(url, data=None)
    req.add_header("User-Agent", "light-arb-spread/1.0")
    if compress:
        req.add_header("Accept-Encoding", "gzip")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
        if r.headers.get("Content-Encoding") == "gzip":
            raw = gzip.decompress(raw)
    return json.loads(raw.decode("utf-8"))


def row(venue, kind, symbol, mark="", index="", last="", bid="", ask="",
        funding="", funding_unit="", oi="", src_ts=""):
    return {
        "ts_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "venue": venue, "kind": kind, "symbol": symbol, "mark": mark, "index": index,
        "last": last, "bid": bid, "ask": ask, "funding": funding,
        "funding_unit": funding_unit, "oi": oi, "src_ts": src_ts,
    }


def poll_lighter() -> list[dict]:
    out = []
    for mid, sym in ((92, "XAU"), (48, "PAXG")):
        d = (_get(f"{LIGHTER}/orderBookDetails?market_id={mid}").get("order_book_details") or [{}])[0]
        out.append(row("lighter", "perp", sym, mark=d.get("mark_price"), index=d.get("index_price"),
                       last=d.get("last_trade_price"), oi=d.get("open_interest")))
    s = (_get(f"{LIGHTER}/orderBookDetails?market_id=2056").get("spot_order_book_details") or [{}])[0]
    out.append(row("lighter", "spot", "XAUT/USDC", last=s.get("last_trade_price"),
                   src_ts=f"trades24h={s.get('daily_trades_count')}"))
    return out


def poll_lighter_funding() -> list[dict]:
    out = []
    f = _get(f"{LIGHTER}/funding-rates")
    for x in f.get("funding_rates", []):
        if x.get("exchange") == "lighter" and x.get("symbol") in ("XAU", "PAXG"):
            out.append(row("lighter", "funding", x.get("symbol"), funding=x.get("rate"),
                           funding_unit="per_1h"))
    return out


def poll_variational() -> list[dict]:
    data = _get(VAR_STATS, timeout=25, compress=True)
    out = []
    for L in data.get("listings", []):
        if L.get("ticker") not in ("XAUT", "XAU", "PAXG"):
            continue
        q = L.get("quotes") or {}
        s1 = q.get("size_1k") or {}
        out.append(row("variational", "perp", L["ticker"], mark=L.get("mark_price"),
                       bid=s1.get("bid"), ask=s1.get("ask"),
                       funding=L.get("funding_rate"), funding_unit="annualized_decimal",
                       oi=(L.get("open_interest") or {}).get("long_open_interest"),
                       src_ts=str(q.get("updated_at", ""))))
    return out


def in_window(now: datetime) -> bool:
    wd, h = now.weekday(), now.hour
    if wd == 4 and h >= 20:      # Fri 20:00 UTC -> Mon 04:00 UTC
        return True
    if wd in (5, 6):
        return True
    if wd == 0 and h < 4:
        return True
    return 11 <= h < 16          # intraday cluster under verification (§7)


def spreads(rows: list[dict]) -> dict[str, float | None]:
    def mark(venue, symbol):
        r = next((x for x in rows if x["venue"] == venue and x["symbol"] == symbol and x["mark"]), None)
        return float(r["mark"]) if r else None

    def last(venue, symbol):
        r = next((x for x in rows if x["venue"] == venue and x["symbol"] == symbol and x["last"]), None)
        return float(r["last"]) if r else None

    xau_l = mark("lighter", "XAU")
    out: dict[str, float | None] = {}
    vx = mark("variational", "XAUT")
    out["vari_xaut_vs_lighter_xau"] = (xau_l - vx) / xau_l * 1e4 if xau_l and vx else None
    px = mark("lighter", "PAXG")
    out["lighter_paxg_vs_xau"] = (xau_l - px) / xau_l * 1e4 if xau_l and px else None
    sx = last("lighter", "XAUT/USDC")
    out["lighter_xautspot_vs_xau"] = (xau_l - sx) / xau_l * 1e4 if xau_l and sx else None
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--outdir", default="data")
    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    next_funding = 0.0
    while True:
        now = datetime.now(timezone.utc)
        t0 = time.time()
        try:
            rows = poll_lighter() + poll_variational()
            sp = spreads(rows)
            if time.time() >= next_funding:
                rows += poll_lighter_funding()
                next_funding = time.time() + 600
            path = os.path.join(args.outdir, f"spread_{now:%Y-%m}.csv")
            new = not os.path.exists(path)
            with open(path, "a", newline="", encoding="utf-8") as fh:
                w = csv.DictWriter(fh, fieldnames=FIELDS)
                if new:
                    w.writeheader()
                w.writerows(rows)
            mx = max((abs(v) for v in sp.values() if v is not None), default=None)
            print(f"{now:%Y-%m-%dT%H:%M:%SZ} rows={len(rows)} "
                  f"vari_xaut={sp['vari_xaut_vs_lighter_xau'] and round(sp['vari_xaut_vs_lighter_xau'], 1)} "
                  f"paxg={sp['lighter_paxg_vs_xau'] and round(sp['lighter_paxg_vs_xau'], 1)} "
                  f"spot={sp['lighter_xautspot_vs_xau'] and round(sp['lighter_xautspot_vs_xau'], 1)} "
                  f"win={int(in_window(now))} "
                  f"trig={int(mx is not None and mx >= TRIGGER_BP)}", flush=True)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError,
                OSError, json.JSONDecodeError, KeyError) as e:
            print(f"{now:%Y-%m-%dT%H:%M:%SZ} ERROR {type(e).__name__}: {e}", flush=True)
        if args.once:
            return
        time.sleep(max(5, MINUTE_S - (time.time() - t0)))


if __name__ == "__main__":
    main()
