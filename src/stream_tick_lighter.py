"""Tick logger via Lighter websocket stream (ticker channels), stdlib only.

Subscribes to ticker/92 (XAU) and ticker/48 (PAXG): best bid/offer pushed on
every matching-engine nonce -- true tick granularity (ms), no polling skew.

Usage:
    python3 src/stream_tick_lighter.py --timeout 30 --outdir data
    python3 src/stream_tick_lighter.py --threshold 1 --outdir data   # continuous

Output: data/tick_stream_xau_paxg_YYYY-MM-DD.csv. To keep volume low, only
interesting ticks are written: |spread| > --threshold plus a 5 s buffer
before/after each case, plus one heartbeat row per minute (hb=1).
Status: DIAGNOSTIC_ONLY, read-only public stream, no auth, no trading.
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import json
import os
import socket
import ssl
import struct
import time
from datetime import datetime, timezone

HOST = "mainnet.zklighter.elliot.ai"
PATH = "/stream"
FIELDS = [
    "ts_utc", "xau_bid", "xau_ask", "paxg_bid", "paxg_ask",
    "spread_entry_bp", "spread_exit_bp", "roundtrip_bp", "case_gt", "hb",
]

PRE_S = 5.0    # buffer before a case
POST_S = 5.0   # trailing window after a case
HEARTBEAT_S = 60.0


def _handshake(sock: socket.socket) -> None:
    key = base64.b64encode(os.urandom(16)).decode()
    req = (
        f"GET {PATH} HTTP/1.1\r\nHost: {HOST}\r\nUpgrade: websocket\r\n"
        f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "User-Agent: light-arb-spread/1.0\r\n\r\n"
    )
    sock.sendall(req.encode())
    fh = sock.makefile("rb")
    head = fh.readline().decode().strip()
    if "101" not in head:
        raise ConnectionError(f"ws handshake failed: {head}")
    while True:
        line = fh.readline().decode().strip()
        if line == "":
            break


def _send_text(sock: socket.socket, text: str) -> None:
    data = text.encode()
    mask = os.urandom(4)
    hdr = bytes([0x81, 0x80 | len(data)]) if len(data) < 126 else struct.pack(
        "!BBH", 0x81, 0x80 | 126, len(data))
    sock.sendall(hdr + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(data)))


def _recv_frames(sock: socket.socket):
    """Yield (opcode, payload) tuples. Handles ping/pong/close/text."""
    while True:
        hdr = sock.recv(2)
        if len(hdr) < 2:
            return
        b1, b2 = hdr[0], hdr[1]
        opcode = b1 & 0x0F
        length = b2 & 0x7F
        if length == 126:
            length = struct.unpack("!H", sock.recv(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", sock.recv(8))[0]
        # server frames are unmasked; read exactly
        payload = b""
        while len(payload) < length:
            chunk = sock.recv(length - len(payload))
            if not chunk:
                return
            payload += chunk
        if opcode == 0x8:  # close
            return
        if opcode == 0x9:  # ping -> pong
            sock.sendall(bytes([0x8A, 0x00]))
            continue
        if opcode == 0xA:  # pong
            continue
        if opcode in (0x1, 0x2):
            yield opcode, payload


def _ensure_header(path: str) -> None:
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as fh:
            csv.DictWriter(fh, fieldnames=FIELDS).writeheader()
        return
    with open(path, "r", encoding="utf-8") as fh:
        head = fh.readline().strip()
    if "hb" not in head.split(","):
        # migrate pre-hb file: old rows get hb=0
        with open(path, "r", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        with open(path, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=FIELDS)
            w.writeheader()
            for r in rows:
                r["hb"] = 0
                w.writerow({k: r.get(k, "") for k in FIELDS})


def _write(path: str, row: dict) -> None:
    with open(path, "a", newline="", encoding="utf-8") as fh:
        csv.DictWriter(fh, fieldnames=FIELDS).writerow(row)


def run(outdir: str, threshold: float, timeout: float) -> int:
    os.makedirs(outdir, exist_ok=True)
    state: dict[int, dict] = {}
    buf: list[tuple[float, dict]] = []  # (mono_ts, row) ring, PRE_S window
    n_seen = n_written = n_cases = 0
    max_abs = 0.0
    last_case_mono: float | None = None
    last_hb = time.time()
    last_stat = time.time()
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = os.path.join(outdir, f"tick_stream_xau_paxg_{day}.csv")
    _ensure_header(path)
    t_end = time.time() + timeout if timeout > 0 else None
    last_ping = time.time()
    while True:
        try:
            raw = socket.create_connection((HOST, 443), timeout=15)
            sock = ssl.create_default_context().wrap_socket(raw, server_hostname=HOST)
            sock.settimeout(20)
            _handshake(sock)
            _send_text(sock, json.dumps({"type": "subscribe", "channel": "ticker/92"}))
            _send_text(sock, json.dumps({"type": "subscribe", "channel": "ticker/48"}))
            print("subscribed ticker/92 ticker/48", flush=True)
            for _, payload in _recv_frames(sock):
                if t_end and time.time() > t_end:
                    return n_written
                if time.time() - last_ping > 60:
                    _send_text(sock, '{"type":"ping"}')
                    last_ping = time.time()
                try:
                    msg = json.loads(payload.decode("utf-8", "replace"))
                except (json.JSONDecodeError, ValueError):
                    continue
                ch = str(msg.get("channel", ""))
                mid = None
                if ch.startswith("ticker:"):
                    try:
                        mid = int(ch.split(":")[1])
                    except ValueError:
                        continue
                if mid not in (92, 48):
                    continue
                tk = msg.get("ticker") or {}
                a = (tk.get("a") or {}).get("price")
                b = (tk.get("b") or {}).get("price")
                try:
                    state[mid] = {"ask": float(a), "bid": float(b)}
                except (TypeError, ValueError):
                    continue
                if 92 not in state or 48 not in state:
                    continue
                xau, paxg = state[92], state[48]
                entry = (xau["bid"] - paxg["ask"]) / xau["bid"] * 1e4
                exit_ = (xau["ask"] - paxg["bid"]) / xau["ask"] * 1e4
                now_mono = time.monotonic()
                n_seen += 1
                max_abs = max(max_abs, abs(entry))
                # Gate on ENTRY only: exit sits structurally above ~2 bp
                # (round-trip cost), gating on it would write everything.
                is_case = abs(entry) > threshold
                row = {
                    "ts_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
                    "xau_bid": xau["bid"], "xau_ask": xau["ask"],
                    "paxg_bid": paxg["bid"], "paxg_ask": paxg["ask"],
                    "spread_entry_bp": round(entry, 2),
                    "spread_exit_bp": round(exit_, 2),
                    "roundtrip_bp": round(entry - exit_, 2),
                    "case_gt": int(is_case),
                    "hb": 0,
                }
                buf.append((now_mono, row))
                buf = [(t, r) for t, r in buf if now_mono - t <= PRE_S]
                if is_case:
                    if last_case_mono is None:  # rising edge: flush pre-buffer
                        for _, br in buf[:-1]:
                            if not br.get("_saved"):
                                br["_saved"] = True
                                _write(path, {k: br[k] for k in FIELDS})
                                n_written += 1
                    row["_saved"] = True
                    _write(path, {k: row[k] for k in FIELDS})
                    n_written += 1
                    n_cases += 1
                    last_case_mono = now_mono
                    for _, br in buf[:-1]:
                        br["_saved"] = True
                    print(f"{row['ts_utc']} CASE entry={row['spread_entry_bp']}bp "
                          f"exit={row['spread_exit_bp']}bp", flush=True)
                elif last_case_mono is not None and now_mono - last_case_mono <= POST_S:
                    row["_saved"] = True  # trailing window
                    _write(path, {k: row[k] for k in FIELDS})
                    n_written += 1
                else:
                    last_case_mono = None
                if time.time() - last_hb >= HEARTBEAT_S:
                    last_hb = time.time()
                    hb = dict(row)
                    hb["hb"] = 1
                    hb["_saved"] = True
                    _write(path, {k: hb[k] for k in FIELDS})
                    n_written += 1
                if time.time() - last_stat >= HEARTBEAT_S:
                    last_stat = time.time()
                    print(f"stat seen={n_seen} written={n_written} "
                          f"cases={n_cases} max|entry|={max_abs:.2f}bp", flush=True)
                if t_end and time.time() > t_end:
                    return n_written
        except (OSError, ConnectionError, TimeoutError) as e:
            print(f"stream ERROR {type(e).__name__}: {e}, reconnect in 3s", flush=True)
            if t_end and time.time() > t_end:
                return n_written
            time.sleep(3)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default="data")
    ap.add_argument("--threshold", type=float, default=1.0)
    ap.add_argument("--timeout", type=float, default=0.0,
                    help="seconds to run, 0 = forever")
    args = ap.parse_args()
    n = run(args.outdir, args.threshold, args.timeout)
    print(f"done n_written={n}", flush=True)


if __name__ == "__main__":
    main()
