"""Fast cross-venue poller: Variational + Hyperliquid + Lighter snapshot, every 15 s.

Variational has no history and no stream (REST stats snapshot only, quotes
cached up to 600 s, measured refresh ~35-60 s), so 15 s polling with dedup by
quotes.updated_at downstream captures everything it publishes. Hyperliquid
PAXG perp is the alternative gold leg (no XAU/XAUT on HL); HL XAUT0 spot is
recorded for curiosity only (wrapped non-canonical token, NOT real XAUT).

Read-only public endpoints, no accounts, no trading. Status: DIAGNOSTIC_ONLY.

Output: data/cross_fast_YYYY-MM-DD.csv (append). One row per instrument per
poll; partial rows when a venue fails (other venues still recorded).

Usage:
    python3 src/poll_cross_fast.py [--once] [--outdir data] [--interval 15]
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

VAR_STATS = "https://omni-client-api.prod.ap-northeast-1.variational.io/metadata/stats"
LIGHTER = "https://mainnet.zklighter.elliot.ai/api/v1"
HL_INFO = "https://api.hyperliquid.xyz/info"

FIELDS = [
    "ts_utc", "venue", "kind", "symbol", "mark", "index", "last",
    "bid", "ask", "bid_100k", "ask_100k", "funding", "funding_unit",
    "oi", "oi_short", "volume_24h", "base_spread_bps", "src_ts",
]


def _get(url: str, timeout: int = 20, compress: bool = False):
    req = urllib.request.Request(url, data=None)
    req.add_header("User-Agent", "light-arb-crossfast/1.0")
    if compress:
        req.add_header("Accept-Encoding", "gzip")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
        if r.headers.get("Content-Encoding") == "gzip":
            raw = gzip.decompress(raw)
    return json.loads(raw.decode("utf-8"))


def _post(url: str, payload: dict, timeout: int = 20):
    req = urllib.request.Request(url, data=json.dumps(payload).encode())
    req.add_header("User-Agent", "light-arb-crossfast/1.0")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def row(venue, kind, symbol, **kw):
    r = {
        "ts_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "venue": venue, "kind": kind, "symbol": symbol, "mark": "", "index": "",
        "last": "", "bid": "", "ask": "", "bid_100k": "", "ask_100k": "",
        "funding": "", "funding_unit": "", "oi": "", "oi_short": "",
        "volume_24h": "", "base_spread_bps": "", "src_ts": "",
    }
    r.update({k: v for k, v in kw.items() if k in r})
    return r


def poll_variational() -> list[dict]:
    data = _get(VAR_STATS, timeout=25, compress=True)
    out = []
    for L in data.get("listings", []):
        if L.get("ticker") not in ("XAUT", "XAU", "PAXG"):
            continue
        q = L.get("quotes") or {}
        s1 = q.get("size_1k") or {}
        s100 = q.get("size_100k") or {}
        oi = L.get("open_interest") or {}
        out.append(row("variational", "perp", L["ticker"], mark=L.get("mark_price"),
                       bid=s1.get("bid"), ask=s1.get("ask"),
                       bid_100k=s100.get("bid"), ask_100k=s100.get("ask"),
                       funding=L.get("funding_rate"), funding_unit="annualized_decimal",
                       oi=oi.get("long_open_interest"), oi_short=oi.get("short_open_interest"),
                       volume_24h=L.get("volume_24h"),
                       base_spread_bps=L.get("base_spread_bps"),
                       src_ts=str(q.get("updated_at", ""))))
    return out


def poll_lighter() -> list[dict]:
    out = []
    for mid, sym in ((92, "XAU"), (48, "PAXG")):
        d = (_get(f"{LIGHTER}/orderBookDetails?market_id={mid}").get("order_book_details") or [{}])[0]
        out.append(row("lighter", "perp", sym, mark=d.get("mark_price"),
                       index=d.get("index_price"), last=d.get("last_trade_price"),
                       oi=d.get("open_interest")))
    return out


def poll_hyperliquid() -> list[dict]:
    out = []
    meta, ctxs = _post(HL_INFO, {"type": "metaAndAssetCtxs"})
    uni = meta.get("universe", [])
    for u, c in zip(uni, ctxs):
        if u.get("name") == "PAXG":
            out.append(row("hyperliquid", "perp", "PAXG", mark=c.get("markPx"),
                           index=c.get("oraclePx"), last=c.get("midPx"),
                           funding=c.get("funding"), funding_unit="per_1h",
                           oi=c.get("openInterest"), volume_24h=c.get("dayNtlVlm")))
            break
    smeta, sctxs = _post(HL_INFO, {"type": "spotMetaAndAssetCtxs"})
    toks = smeta.get("tokens", [])
    for u, c in zip(smeta.get("universe", []), sctxs):
        try:
            names = [toks[t].get("name") for t in u.get("tokens", [])]
        except (IndexError, TypeError, AttributeError):
            continue
        if len(names) == 2 and names[0] == "XAUT0" and names[1] == "USDC":
            out.append(row("hyperliquid", "spot", "XAUT0/USDC", mark=c.get("markPx"),
                           last=c.get("midPx"), volume_24h=c.get("dayNtlVlm"),
                           src_ts="wrapped-noncanonical-curiosity"))
            break
    return out


def spreads(rows: list[dict]) -> dict[str, float | None]:
    def mark(venue, symbol):
        r = next((x for x in rows if x["venue"] == venue and x["symbol"] == symbol
                  and x["kind"] == "perp" and x["mark"]), None)
        return float(r["mark"]) if r else None

    lx = mark("lighter", "XAU")
    out: dict[str, float | None] = {}
    for key, venue, sym in (("vari_xaut", "variational", "XAUT"),
                            ("vari_xau", "variational", "XAU"),
                            ("vari_paxg", "variational", "PAXG"),
                            ("hl_paxg", "hyperliquid", "PAXG")):
        v = mark(venue, sym)
        out[key] = (lx - v) / lx * 1e4 if lx and v else None
    vx = mark("variational", "XAUT")
    va = mark("variational", "XAU")
    out["vari_xaut_vs_xau"] = (va - vx) / va * 1e4 if va and vx else None
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--outdir", default="data")
    ap.add_argument("--interval", type=float, default=15)
    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    while True:
        now = datetime.now(timezone.utc)
        t0 = time.time()
        try:
            rows, errs = [], []
            for fn in (poll_variational, poll_lighter, poll_hyperliquid):
                try:
                    rows += fn()
                except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError,
                        OSError, json.JSONDecodeError, KeyError, ValueError) as e:
                    errs.append(f"{fn.__name__}:{type(e).__name__}")
            path = os.path.join(args.outdir, f"cross_fast_{now:%Y-%m-%d}.csv")
            new = not os.path.exists(path)
            with open(path, "a", newline="", encoding="utf-8") as fh:
                w = csv.DictWriter(fh, fieldnames=FIELDS)
                if new:
                    w.writeheader()
                w.writerows(rows)
            sp = spreads(rows)
            fmt = lambda v: round(v, 1) if v is not None else None
            print(f"{now:%Y-%m-%dT%H:%M:%SZ} rows={len(rows)} "
                  f"vari_xaut={fmt(sp['vari_xaut'])} vari_xau={fmt(sp['vari_xau'])} "
                  f"hl_paxg={fmt(sp['hl_paxg'])} vari_xaut_vs_xau={fmt(sp['vari_xaut_vs_xau'])} "
                  + (f"errors={','.join(errs)}" if errs else "ok"), flush=True)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError,
                OSError, json.JSONDecodeError, KeyError) as e:
            print(f"{now:%Y-%m-%dT%H:%M:%SZ} ERROR {type(e).__name__}: {e}", flush=True)
        if args.once:
            return
        time.sleep(max(5, args.interval - (time.time() - t0)))


if __name__ == "__main__":
    main()
