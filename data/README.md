# data/

Что лежит (полные файлы — вне гита, в гите только семплы `*_sample.csv`):

- `spread_YYYY-MM.csv` — снимки рекордера (`src/record_spread.py`), append, CSV с заголовком. Одна строка = одна площадка за один опрос.
- `lighter_{xau,paxg,xautspot}_1m.csv` — нативные 1-минутные свечи Lighter (`src/backfill_lighter_1m.py`): `ts_utc,open,high,low,close,vol`.
- `spread_2026-09_salvaged_log.csv` — минутный ряд спредов за 12.09 19:52 → 13.09 14:04 (только агрегаты, без сырых котировок).
- `spread_sample.csv`, `lighter_xau_1m_sample.csv` — семплы для гита.
- `funding_YYYY-MM.csv` — кросс-площадочный фандинг (`src/record_funding.py`, опрос каждых 10 мин): `ts_utc,exchange,symbol,market_id,rate,rate_bp`. Символы XAU/PAXG/XAG × площадки lighter/binance/bybit/hyperliquid (один эндпоинт Lighter). Единицы сырые, как отдаёт источник, — без конверсии не смешивать.
- `funding_binance_paxg_hist.csv` — история фандинга Binance PAXGUSDT (разовый бэкфилл 17.09, 500 точек с 26.06, интервал ~4 ч), тот же формат.

Схема снимков:

| Поле | Смысл |
|---|---|
| `ts_utc` | время записи (UTC, часы нашего процесса) |
| `venue` | `lighter`, `variational` |
| `kind` | `perp` (ценовые поля), `spot` (XAUT/USDC), `funding` (только ставка) |
| `symbol` | `XAU`, `PAXG`, `XAUT`, `XAUT/USDC` |
| `mark`, `index`, `last` | цена маркировки / индекс / последняя сделка |
| `bid`, `ask` | лучшие цены (для `variational` — котировка OLP на размер 1k) |
| `funding` | текущая ставка фандинга |
| `funding_unit` | `per_8h`, `annualized_decimal` (Variational: ×100 = % годовых) |
| `oi` | открытый интерес (для Variational — длинный OI) |
| `src_ts` | метка времени источника (если отдаётся) |

Остальное (зачем собирается, как считается спред, покрытие по датам) — в `../docs/xaut-xau-basis-context.md` (§2, §4).
