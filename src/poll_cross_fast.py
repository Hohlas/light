"""Fast cross-venue poller: Variational + Hyperliquid + Lighter snapshot +
Aster + Paradex + ApeX, every 15 s.

Tracks: gold (XAUT/XAU/PAXG), silver (XAG), platinum/palladium (XPT/XPD),
oil (WTI) and gas (NATGAS) as trade candidates; BTC/ETH/SOL as control legs;
1000PEPE as long-tail pilot. Ostium deliberately excluded; COPPER skipped
(single venue, no pair).

Name normalization (one table per venue below): CL/WTI/CLUSDT -> WTI,
NG/NATGASUSDT -> NATGAS. Everything else matches across venues as-is.

Venue notes (verified live):
- Variational: REST stats snapshot only, quotes cached up to 600 s
  (measured refresh ~35-60 s, marks ~5 min) -> dedup downstream.
- Paradex: public bbo only; XAU/XAG/XPT books are stubs (bid/ask several
  times apart) -> depeg-prone, needs a confirming leg at analysis time.
  No XPD contract. Klines need auth -> live only.
- ApeX: no XPT/XPD contracts; klines return latest ~200 min only.
- HL: PAXG perp + XAUT0 spot (wrapped, curiosity); BTC/ETH/SOL perps.

Read-only public endpoints, no accounts, no trading. Status: DIAGNOSTIC_ONLY.

Output: data/cross_fast_YYYY-MM-DD.csv (append). One row per instrument per
poll; partial rows when a venue fails (other venues still recorded).
src_ts for book-based rows packs sizes: 'ts=...;bid_sz=...;ask_sz=...'.

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
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

VAR_STATS = "https://omni-client-api.prod.ap-northeast-1.variational.io/metadata/stats"
LIGHTER = "https://mainnet.zklighter.elliot.ai/api/v1"
HL_INFO = "https://api.hyperliquid.xyz/info"
ASTER = "https://fapi.asterdex.com"
PARADEX = "https://api.prod.paradex.trade/v1"
APEX = "https://omni.apex.exchange/api/v3"

VAR_TICKERS = ("XAUT", "XAU", "PAXG", "XAG", "XPT", "XPD", "CL", "NATGAS",
                "BTC", "ETH", "SOL", "1000PEPE",
                "AAPL", "AMZN", "COIN", "HOOD", "META", "MSFT", "NVDA",
                "QQQ", "SPX", "TSLA")  # stocks since 2026-09-16 (14d gate clock)
VAR_NORM = {"CL": "WTI"}  # venue ticker -> canonical symbol
LIGHTER_MIDS = ((92, "XAU"), (48, "PAXG"), (93, "XAG"), (147, "XPT"),
                (146, "XPD"), (145, "WTI"), (158, "NATGAS"), (1, "BTC"),
                (0, "ETH"), (2, "SOL"), (4, "1000PEPE"),
                (113, "AAPL"), (114, "AMZN"), (109, "COIN"), (108, "HOOD"),
                (117, "META"), (115, "MSFT"), (110, "NVDA"), (129, "QQQ"),
                (42, "SPX"), (112, "TSLA"))
ASTER_SYMS = (("XAUUSDT", "XAU"), ("PAXGUSDT", "PAXG"), ("XAGUSDT", "XAG"),
              ("XPTUSDT", "XPT"), ("XPDUSDT", "XPD"), ("CLUSDT", "WTI"),
              ("NATGASUSDT", "NATGAS"), ("BTCUSDT", "BTC"), ("ETHUSDT", "ETH"),
              ("SOLUSDT", "SOL"), ("1000PEPEUSDT", "1000PEPE"),
              ("AAPLUSDT", "AAPL"), ("AMZNUSDT", "AMZN"), ("COINUSDT", "COIN"),
              ("HOODUSDT", "HOOD"), ("METAUSDT", "META"), ("MSFTUSDT", "MSFT"),
              ("NVDAUSDT", "NVDA"), ("QQQUSDT", "QQQ"), ("SPXUSDT", "SPX"),
              ("TSLAUSDT", "TSLA"))
PARADEX_SYMS = (("XAU-USD-PERP", "XAU"), ("PAXG-USD-PERP", "PAXG"),
                ("XAG-USD-PERP", "XAG"), ("XPT-USD-PERP", "XPT"),
                ("CL-USD-PERP", "WTI"), ("NG-USD-PERP", "NATGAS"),
                ("BTC-USD-PERP", "BTC"), ("ETH-USD-PERP", "ETH"),
                ("SOL-USD-PERP", "SOL"))
APEX_SYMS = (("XAUUSDT", "XAU"), ("PAXGUSDT", "PAXG"), ("XAGUSDT", "XAG"),
             ("CLUSDT", "WTI"), ("NATGASUSDT", "NATGAS"),
             ("1000PEPEUSDT", "1000PEPE"), ("BTCUSDT", "BTC"),
             ("ETHUSDT", "ETH"), ("SOLUSDT", "SOL"))
HL_PERPS = ("PAXG", "BTC", "ETH", "SOL")

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


def _fetch_all(items: list, fetch, workers: int = 10) -> tuple[list, int]:
    """Concurrent GETs (I/O-bound); returns (results in order, fail count).

    Per-item error tag: 'HTTP<code>:<body-head>' for HTTP errors (first 120
    chars of the response body -- enough to tell throttling apart from an
    empty market or maintenance), else the exception type name.
    """
    def safe(it):
        try:
            return (it, fetch(it), None)
        except urllib.error.HTTPError as e:
            try:
                body = e.read(120).decode("utf-8", "replace")
            except Exception:  # noqa: BLE001 - best effort diagnostics
                body = ""
            return (it, None, f"HTTP{e.code}:{body[:120]}")
        except Exception as e:  # noqa: BLE001 - per-item isolation
            return (it, None, type(e).__name__)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        res = list(ex.map(safe, items))
    return res, sum(1 for _, _, e in res if e)


MISSED: list[str] = []  # per-poll silently skipped legs (venue:sym), reset each cycle, log only


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
        if L.get("ticker") not in VAR_TICKERS:
            continue
        sym = VAR_NORM.get(L["ticker"], L["ticker"])
        q = L.get("quotes") or {}
        s1 = q.get("size_1k") or {}
        s100 = q.get("size_100k") or {}
        oi = L.get("open_interest") or {}
        out.append(row("variational", "perp", sym, mark=L.get("mark_price"),
                       bid=s1.get("bid"), ask=s1.get("ask"),
                       bid_100k=s100.get("bid"), ask_100k=s100.get("ask"),
                       funding=L.get("funding_rate"), funding_unit="annualized_decimal",
                       oi=oi.get("long_open_interest"), oi_short=oi.get("short_open_interest"),
                       volume_24h=L.get("volume_24h"),
                       base_spread_bps=L.get("base_spread_bps"),
                       src_ts=str(q.get("updated_at", ""))))
    return out


def _get_retry(url: str, timeout: int = 20, tries: int = 3, compress: bool = False):
    """Retries: Lighter intermittently drops single markets (transient)."""
    err: Exception | None = None
    for _ in range(tries):
        try:
            return _get(url, timeout=timeout, compress=compress)
        except Exception as e:  # noqa: BLE001 - retry then propagate
            err = e
    raise err  # type: ignore[misc]


def poll_lighter() -> list[dict]:
    """One batched orderBookDetails call (all 235 markets) mapped by market_id.

    Missing legs are emitted as "" rows (see data/README.md invariant), never
    skipped: absent row = poll did not happen, empty mark = leg missing.
    """
    out = []
    d = _get_retry(f"{LIGHTER}/orderBookDetails", timeout=25, compress=True)
    by_mid = {x.get("market_id"): x for x in (d.get("order_book_details") or [])}
    for mid, sym in LIGHTER_MIDS:
        m = by_mid.get(mid)
        if not m or not m.get("mark_price"):
            MISSED.append(f"lighter:{sym}")
            out.append(row("lighter", "perp", sym))
            continue
        out.append(row("lighter", "perp", sym, mark=m.get("mark_price"),
                       index=m.get("index_price"), last=m.get("last_trade_price"),
                       oi=m.get("open_interest")))
    return out


def poll_hyperliquid() -> list[dict]:
    out = []
    meta, ctxs = _post(HL_INFO, {"type": "metaAndAssetCtxs"})
    uni = meta.get("universe", [])
    for u, c in zip(uni, ctxs):
        if u.get("name") in HL_PERPS:
            out.append(row("hyperliquid", "perp", u["name"], mark=c.get("markPx"),
                           index=c.get("oraclePx"), last=c.get("midPx"),
                           funding=c.get("funding"), funding_unit="per_1h",
                           oi=c.get("openInterest"), volume_24h=c.get("dayNtlVlm")))
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


def poll_aster() -> list[dict]:
    """Three batched calls (bookTicker / 24hr / premiumIndex, all symbols).

    Missing legs are emitted as "" rows (see data/README.md invariant).
    """
    out = []
    books = {b.get("symbol"): b for b in _get_retry(f"{ASTER}/fapi/v1/ticker/bookTicker", compress=True)}
    days = {t.get("symbol"): t for t in _get_retry(f"{ASTER}/fapi/v1/ticker/24hr", compress=True)}
    prems = {p.get("symbol"): p for p in _get_retry(f"{ASTER}/fapi/v1/premiumIndex", compress=True)}
    for sym, norm in ASTER_SYMS:
        b, t, p = books.get(sym, {}), days.get(sym, {}), prems.get(sym, {})
        if not (b or t or p):
            MISSED.append(f"aster:{sym}")
            out.append(row("aster", "perp", norm))
            continue
        out.append(row("aster", "perp", norm, mark=t.get("lastPrice") or p.get("markPrice"),
                       index=p.get("indexPrice"), last=t.get("lastPrice"),
                       bid=b.get("bidPrice"), ask=b.get("askPrice"),
                       funding=p.get("lastFundingRate"), funding_unit="unknown",
                       volume_24h=t.get("quoteVolume") or t.get("volume"),
                       src_ts=f"ts={b.get('time', '')};bid_sz={b.get('bidQty', '')};"
                              f"ask_sz={b.get('askQty', '')}"))
    return out


def poll_paradex() -> list[dict]:
    out = []
    res, _ = _fetch_all(list(PARADEX_SYMS),
                        lambda it: _get(f"{PARADEX}/bbo/{it[0]}"))
    for (market, norm), b, e in res:
        if e or not b:
            MISSED.append(f"paradex:{norm}" + (f":{e}" if e else ""))
            out.append(row("paradex", "perp", norm))
            continue
        try:
            mid = (float(b.get("bid")) + float(b.get("ask"))) / 2
        except (TypeError, ValueError):
            mid = ""
        out.append(row("paradex", "perp", norm, mark=mid, bid=b.get("bid"), ask=b.get("ask"),
                       src_ts=f"ts={b.get('last_updated_at', '')};bid_sz={b.get('bid_size', '')};"
                              f"ask_sz={b.get('ask_size', '')}"))
    return out


def _apex_bundle(it) -> dict:
    sym, norm = it
    t = (_get(f"{APEX}/ticker?symbol={sym}").get("data") or [{}])[0]
    d = (_get(f"{APEX}/depth?symbol={sym}&limit=1").get("data") or {})
    return {"norm": norm, "t": t, "d": d}


def poll_apex() -> list[dict]:
    out = []
    res, _ = _fetch_all(list(APEX_SYMS), _apex_bundle)
    for (sym, norm), r, e in res:
        if e or not r:
            MISSED.append(f"apex:{sym}" + (f":{e}" if e else ""))
            out.append(row("apex", "perp", norm))
            continue
        t, d = r["t"], r["d"]
        asks, bids = d.get("a") or [], d.get("b") or []
        bb = bids[0] if bids else [None, None]
        ba = asks[0] if asks else [None, None]
        out.append(row("apex", "perp", r["norm"], mark=t.get("markPrice"), index=t.get("indexPrice"),
                       last=t.get("lastPrice"), bid=bb[0], ask=ba[0],
                       funding=t.get("fundingRate"), funding_unit="per_1h",
                       oi=t.get("openInterest"),
                       volume_24h=t.get("quoteVolume") or t.get("turnover24h"),
                       src_ts=f"bid_sz={bb[1] or ''};ask_sz={ba[1] or ''}"))
    return out


def spreads(rows: list[dict]) -> dict[str, float | None]:
    def mark(venue, symbol):
        r = next((x for x in rows if x["venue"] == venue and x["symbol"] == symbol
                  and x["kind"] == "perp" and x["mark"]), None)
        return float(r["mark"]) if r else None

    def bp(base, v):
        return (base - v) / base * 1e4 if base and v else None

    lx, lxag = mark("lighter", "XAU"), mark("lighter", "XAG")
    out: dict[str, float | None] = {}
    for key, venue, sym in (("vari_xaut", "variational", "XAUT"),
                            ("vari_xau", "variational", "XAU"),
                            ("hl_paxg", "hyperliquid", "PAXG"),
                            ("aster_xau", "aster", "XAU"),
                            ("aster_paxg", "aster", "PAXG"),
                            ("apex_xau", "apex", "XAU"),
                            ("apex_paxg", "apex", "PAXG"),
                            ("pdax_xau", "paradex", "XAU"),
                            ("pdax_paxg", "paradex", "PAXG")):
        out[key] = bp(lx, mark(venue, sym))
    for key, venue in (("vari_xag", "variational"), ("aster_xag", "aster"),
                       ("apex_xag", "apex"), ("pdax_xag", "paradex")):
        out[key] = bp(lxag, mark(venue, "XAG"))
    for key, venue, sym in (("vari_xpt", "variational", "XPT"),
                            ("vari_xpd", "variational", "XPD"),
                            ("vari_wti", "variational", "WTI"),
                            ("vari_gas", "variational", "NATGAS"),
                            ("vari_btc", "variational", "BTC")):
        out[key] = bp(mark("lighter", sym), mark(venue, sym))
    vx, va = mark("variational", "XAUT"), mark("variational", "XAU")
    out["vari_xaut_vs_xau"] = bp(va, vx)
    for sym in ("AAPL", "AMZN", "COIN", "HOOD", "META", "MSFT", "NVDA",
                "QQQ", "SPX", "TSLA"):
        out[f"eq_{sym.lower()}"] = bp(mark("lighter", sym), mark("aster", sym))
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
            del MISSED[:]
            for fn in (poll_variational, poll_lighter, poll_hyperliquid,
                       poll_aster, poll_paradex, poll_apex):
                try:
                    rows += fn()
                except urllib.error.HTTPError as e:
                    try:
                        body = e.read(120).decode("utf-8", "replace")
                    except Exception:  # noqa: BLE001 - best effort diagnostics
                        body = ""
                    errs.append(f"{fn.__name__}:HTTP{e.code}:{body[:120]}")
                except (urllib.error.URLError, TimeoutError,
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
            cyc = round(time.time() - t0, 1)
            print(f"{now:%Y-%m-%dT%H:%M:%SZ} rows={len(rows)} cyc={cyc}s "
                  f"vari_xaut={fmt(sp['vari_xaut'])} hl={fmt(sp['hl_paxg'])} "
                  f"aster_xau={fmt(sp['aster_xau'])} apex_paxg={fmt(sp['apex_paxg'])} "
                  f"pdax_paxg={fmt(sp['pdax_paxg'])} vari_xag={fmt(sp['vari_xag'])} "
                  f"vari_xpt={fmt(sp['vari_xpt'])} vari_wti={fmt(sp['vari_wti'])} "
                  f"vari_btc={fmt(sp['vari_btc'])} "
                   f"eq_qqq={fmt(sp['eq_qqq'])} eq_aapl={fmt(sp['eq_aapl'])} "
                   f"eq_tsla={fmt(sp['eq_tsla'])} eq_nvda={fmt(sp['eq_nvda'])} "
                   + (f"errors={','.join(errs)}" if errs else "ok")
                   + (f" miss={','.join(MISSED)}" if MISSED else ""), flush=True)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError,
                OSError, json.JSONDecodeError, KeyError) as e:
            print(f"{now:%Y-%m-%dT%H:%M:%SZ} ERROR {type(e).__name__}: {e}", flush=True)
        if args.once:
            return
        time.sleep(max(5, args.interval - (time.time() - t0)))


if __name__ == "__main__":
    main()
