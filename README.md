# light — мониторинг базиса XAUT ↔ XAU

Проверка сделки «Variational XAUT (Long) — Lighter XAU (Short)».

Статус: RESEARCH_ONLY / DIAGNOSTIC_ONLY, результатов нет.

Контекст: `docs/xaut-xau-basis-context.md`. Данные: `data/README.md`.


```bash
install -m 600 /dev/null /mnt/keys/api   # Создать файл ключей с доступом только владельцу.
nano /mnt/keys/api                        # Внести секреты вручную, вне git и истории команд.
```

```bash
LIGHTER_API_KEY='API_KEY'       # Публичная часть ключа Lighter, подставить свою.
LIGHTER_API_SECRET='API_SECRET' # Секретная часть, одинарные кавычки гасят спецсимволы.
```

```bash

set -a                           # Включить автоэкспорт всех задаваемых переменных.
source "/mnt/keys/api"           # Прочитать файл: только присваивания, без лишних строк.
set +a                           # Выключить автоэкспорт, чтобы не утекало дальше.
test -n "$LIGHTER_API_KEY" && test -n "$LIGHTER_API_SECRET" && echo keys-ok  # Проверить наличие обоих ключей, не печатая значений.

```

```bash
exec python3 light_bot.py        # Запустить бота с подменой шелла, ключи достанутся из окружения.
```