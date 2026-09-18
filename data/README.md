# data/

Полные файлы — вне гита (в гите только семплы `*_sample.csv`):

- `spread_YYYY-MM.csv` — минутный рекордер (`src/record_spread.py`).
- `cross_fast_ГГГГ-ММ-ДД.csv` — кросс-опрос 6 площадок, 15 с, 87 строк/опрос (`src/poll_cross_fast.py`).
- `lighter_{xau,paxg,xautspot}_1m.csv` — минутные свечи Lighter: `ts_utc,open,high,low,close,vol`.
- `funding_YYYY-MM.csv` — фандинг XAU/PAXG/XAG × 4 площадки, 10 мин (`src/record_funding.py`): `ts_utc,exchange,symbol,market_id,rate,rate_bp` (единицы сырые — не смешивать без конверсии).
- `funding_binance_paxg_hist.csv` — история фандинга Binance PAXGUSDT с 26.06 (тот же формат).
- `cex_books_ГГГГ-ММ-ДД.csv` — книги Binance/Bybit, 60 с (`src/poll_cex_books.py`): `ts_utc,venue,market,symbol,bid,ask,bid_qty,ask_qty,bid_not5_usd,ask_not5_usd,spread_bp,src_ts`.
- `spread_2026-09_salvaged_log.csv` — агрегаты 12.09 19:52 → 13.09 14:04 (без сырых котировок).

Схема `spread_*` / `cross_fast_*`:

| Поле | Смысл |
|---|---|
| `ts_utc` | время записи, UTC (в `cross_fast_*` порядочное — не ключ опроса) |
| `venue` | `variational`, `lighter`, `hyperliquid`, `aster`, `paradex`, `apex` |
| `kind` | `perp`, `spot`, `funding` (только `spread_*`) |
| `symbol` | `XAU`, `PAXG`, `XAG`, `XPT`, `XPD`, `WTI`, `NATGAS`, `BTC`, `ETH`, `SOL`, `1000PEPE`, `AAPL…TSLA`, `XAUT`, `XAUT/USDC`, `XAUT0/USDC` |
| `mark`, `index`, `last` | маркировка / индекс / последняя сделка |
| `bid`, `ask` | лучшие цены (variational — OLP на 1k) |
| `bid_100k`, `ask_100k`, `oi_short`, `volume_24h`, `base_spread_bps` | только `cross_fast_*` |
| `funding`, `funding_unit` | ставка; `per_8h` / `per_1h` / `annualized_decimal` |
| `oi` | открытый интерес |
| `src_ts` | метка источника; размеры как `ts=…;bid_sz=…;ask_sz=…` |

Пропуски (`cross_fast_*`): пустой `mark` = ноги не было в опросе. До 2026-09-18T07:24:02Z — отсутствие строки, после — пустой `mark`. Считать только непустые `mark`; полнота — по `miss=` в `/tmp/light_crossfast.log`.

Остальное — в `../docs/xaut-xau-basis-context.md` (§2, §4).
