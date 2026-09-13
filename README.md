# light — мониторинг базиса XAUT ↔ XAU

Сделка «**Variational XAUT (Long) — Lighter XAU (Short)**»: базисный трейд между
токеном Tether Gold и золотом через два перпетуала. Это **не** арбитраж одного
актива — это трейд спреда между разными активами, с базисным риском.

- **Статус**: `RESEARCH_ONLY` / `DIAGNOSTIC_ONLY`. Сделок нет, счетов нет —
  только публичные read-only API. Торговых выводов из этих данных не делается.
- **Контекст целиком**: `docs/xaut-xau-basis-context.md` — самодостаточная
  выгрузка (механизм, измерения, гейты, риски, открытые вопросы).
- Происхождение: выделено 2026-09-13 из `Hohlas/SoSimple`
  ([arbitrage-directions](https://github.com/Hohlas/SoSimple/blob/main/docs/superpowers/specs/2026-09-12-arbitrage-directions.md)).

## Текущий вывод (2026-09-13)

Возможности нет: спред +10…+18 bp против медианы +34…+37 bp (ниже нормы —
«сужение», а не «расширение»). Триггер входа: `s ≥ +87 bp`, выход при возврате
к `+40…+50 bp` (предлагаемые, не проверенные пороги). Критерии KILL — в §9
контекстного дока.

## Структура

```
light/
├── README.md            # этот файл
├── requirements.txt     # только стандартная библиотека (Python ≥ 3.10)
├── src/
│   ├── record_spread.py       # минутный рекордер: Variational + Lighter → data/
│   └── backfill_lighter_1m.py # бэкфилл нативных 1-минутных свечей Lighter
├── docs/
│   └── xaut-xau-basis-context.md
└── data/
    ├── README.md              # формат данных, как считать спред
    ├── spread_sample.csv      # семпл снимков рекордера
    └── lighter_xau_1m_sample.csv  # семпл минуток Lighter
```

Полные данные в git не хранятся (см. `data/README.md`) — воспроизводятся
скриптами из `src/`.

## Запуск

```bash
python3 src/record_spread.py --once --outdir data   # разовый снимок
python3 src/record_spread.py --outdir data           # непрерывная запись, опрос 60 с
python3 src/backfill_lighter_1m.py --days 7 --outdir data  # минутки Lighter
```

Спред в базисных пунктах:

```text
s_bp = (lighter_XAU_mark − XAUT_mark) / lighter_XAU_mark × 10_000
```

«+» = XAUT дешевле золота (расширение — состояние для входа по логике автора).
