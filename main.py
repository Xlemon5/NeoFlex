import csv
import os
import math
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv

"""
CSV → PostgreSQL bulk‑loader + ETL‑логирование
==============================================

* Удаляет точные дубликаты строк.
* Нормализует заголовки (`snake_case`).
* Приводит даты к ISO и отбрасывает строки с ошибочными датами.
* Валидирует/обрезает значения под ограничения VARCHAR‑полей.
* Преобразует `numpy.*` скаляры и `NaN` → Python‑типы.
* **Логирует** начало/конец в `logs.etl_runs` (таблица уже существует).
* После записи «start» делает паузу 5 сек, чтобы увидеть разницу во времени.

Таблица логов
-------------
```sql
CREATE SCHEMA IF NOT EXISTS logs;

CREATE TABLE IF NOT EXISTS logs.etl_runs (
    run_id        BIGSERIAL PRIMARY KEY,
    dag_id        text      NOT NULL,
    task_id       text      NOT NULL,
    started_at    timestamptz NOT NULL DEFAULT now(),
    finished_at   timestamptz,
    status        text,
    rows_loaded   bigint,
    note          text
);
```

Запуск
------
```bash
python csv_to_pg_loader.py /path/to/csv_dir
```
Если каталог не указан, используется `DATA_DIR` в коде.
"""

# --------------------------------------------------------------------------- #
# 1. Конфигурация                                                             #
# --------------------------------------------------------------------------- #
load_dotenv()

conn_params = {
    "host":     os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

DATA_DIR = Path("/Users/ilya/Desktop/neoFlex/data")  # default CSV location

DAG_ID   = "csv_loader"     # логический DAG / процесс
TASK_ID  = "bulk_load_csv"   # шаг внутри DAG
LOG_TABLE = "logs.etl_runs"  # где пишем логи

csv_files = {
    "md_ledger_account_s": DATA_DIR / "md_ledger_account_s.csv",
    "md_exchange_rate_d":  DATA_DIR / "md_exchange_rate_d.csv",
    "ft_balance_f":        DATA_DIR / "ft_balance_f.csv",
    "md_account_d":        DATA_DIR / "md_account_d.csv",
    "md_currency_d":       DATA_DIR / "md_currency_d.csv",
    "ft_posting_f":        DATA_DIR / "ft_posting_f.csv",
}

# --------------------------------------------------------------------------- #
# 2. INSERT‑шаблоны                                                           #
# --------------------------------------------------------------------------- #
insert_queries = {
    "md_ledger_account_s": (
        "INSERT INTO ds.md_ledger_account_s ("
        "chapter, chapter_name, section_number, section_name, subsection_name, "
        "ledger1_account, ledger1_account_name, ledger_account, ledger_account_name, "
        "characteristic, start_date, end_date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT DO NOTHING;"
    ),
    "md_exchange_rate_d": (
        "INSERT INTO ds.md_exchange_rate_d ("
        "data_actual_date, data_actual_end_date, currency_rk, reduced_cource, code_iso_num) "
        "VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;"
    ),
    "ft_balance_f": (
        "INSERT INTO ds.ft_balance_f ("
        "on_date, account_rk, currency_rk, balance_out) VALUES (%s,%s,%s,%s) "
        "ON CONFLICT DO NOTHING;"
    ),
    "md_account_d": (
        "INSERT INTO ds.md_account_d ("
        "data_actual_date, data_actual_end_date, account_rk, account_number, "
        "char_type, currency_rk, currency_code) VALUES (%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT DO NOTHING;"
    ),
    "md_currency_d": (
        "INSERT INTO ds.md_currency_d ("
        "currency_rk, data_actual_date, data_actual_end_date, currency_code, code_iso_char) "
        "VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;"
    ),
    "ft_posting_f": (
        "INSERT INTO ds.ft_posting_f ("
        "oper_date, credit_account_rk, debet_account_rk, credit_amount, debet_amount) "
        "VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;"
    ),
}

# --------------------------------------------------------------------------- #
# 3. Форматы дат и ограничения VARCHAR                                        #
# --------------------------------------------------------------------------- #

date_columns = {
    "ft_balance_f": {"on_date": "DD.MM.YYYY"},
    "ft_posting_f": {"oper_date": "DD-MM-YYYY"},
}

varchar_limits = {
    "md_currency_d": {"currency_code": 3, "code_iso_char": 3},
}

# --------------------------------------------------------------------------- #
# 4. Утилиты                                                                  #
# --------------------------------------------------------------------------- #

def snake_case(name: str) -> str:
    return name.strip().replace(" ", "_").replace("-", "_").lower()


def convert_date(date_str: str, fmt: str) -> str | None:
    if not date_str or str(date_str).lower() in {"nan", "none"}:
        return None
    try:
        if fmt == "DD.MM.YYYY":
            return datetime.strptime(date_str, "%d.%m.%Y").strftime("%Y-%m-%d")
        if fmt == "DD-MM-YYYY":
            return datetime.strptime(date_str, "%d-%m-%Y").strftime("%Y-%m-%d")
        if fmt == "YYYY-MM-DD":
            return date_str
    except ValueError:
        pass
    return None


def pythonify(value):
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, np.generic):
        return value.item()
    return value

# --------------------------------------------------------------------------- #
# 5. Подготовка DataFrame                                                     #
# --------------------------------------------------------------------------- #

def prepare_dataframe(table: str, path: Path) -> pd.DataFrame:
    # пробуем несколько кодировок
    for enc in ("utf-8", "utf-8-sig", "cp1251", "latin-1"):
        try:
            df = pd.read_csv(path, delimiter=";", encoding=enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise UnicodeDecodeError("Не удалось открыть файл", path, 0, 0, "кодировки")

    df.columns = [snake_case(c) for c in df.columns]

    # дубликаты
    dup_mask = df.duplicated(keep=False)
    if dup_mask.any():
        print(f"[WARN] {path.name}: найдено {dup_mask.sum()} дубликат(ов); они будут пропущены.")
        df = df[~dup_mask]

    # даты
    if table in date_columns:
        for col, fmt in date_columns[table].items():
            if col in df.columns:
                df[col] = df[col].apply(lambda x: convert_date(str(x), fmt))
        df = df.dropna(subset=date_columns[table].keys())

    # ограничения VARCHAR
    if table in varchar_limits:
        for col, max_len in varchar_limits[table].items():
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str[:max_len]
                df[col] = df[col].replace({"": None})

    return df

# --------------------------------------------------------------------------- #
# 6. Логирование                                                              #
# --------------------------------------------------------------------------- #

def log_start(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {LOG_TABLE} (dag_id, task_id, status) "
            "VALUES (%s, %s, 'running') RETURNING run_id;",
            (DAG_ID, TASK_ID)
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    print(f"[LOG] run_id={run_id} started")
    return run_id


def log_finish(conn, run_id: int, status: str, rows: int | None = None, note: str | None = None):
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {LOG_TABLE} "
            "SET finished_at = now(), status = %s, rows_loaded = %s, note = %s "
            "WHERE run_id = %s;",
            (status, rows, note, run_id)
        )
    conn.commit()
    print(f"[LOG] run_id={run_id} finished with status={status} rows={rows}")

# --------------------------------------------------------------------------- #
# 7. Загрузка одной таблицы                                                   #
# --------------------------------------------------------------------------- #

def import_table(table: str, path: Path, conn) -> int:
    """Читает CSV → DataFrame → вставляет. Возвращает кол-во загруженных строк."""
    if not path.exists():
        print(f"[SKIP] {path} не найден")
        return 0

    df = prepare_dataframe(table, path)
    if df.empty:
        print(f"[SKIP] {path.name}: после очистки строк не осталось")
        return 0

    # упорядочиваем колонки как в VALUES (...) шаблона
    cols_in_query = insert_queries[table].split("(")[1].split(")")[0]
    cols = [snake_case(c.strip()) for c in cols_in_query.split(",")]

    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{path.name}: отсутствуют колонки {missing}")

    df = df[cols]
    df = df.where(pd.notnull(df), None)

    rows_data = [[pythonify(v) for v in row] for row in df.to_numpy()]

    with conn.cursor() as cur:
        cur.executemany(insert_queries[table], rows_data)
    print(f"[OK] {table}: загружено {len(rows_data)} строк")
    return len(rows_data)

# --------------------------------------------------------------------------- #
# 8. main                                                                     #
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    import sys

    root_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else DATA_DIR

    conn = psycopg2.connect(**conn_params)
    try:
        run_id = log_start(conn)      # статус running
        time.sleep(5)                # искусственная пауза для наглядности

        total_rows = 0
        for tbl, default_path in csv_files.items():
            path = root_dir / default_path.name if root_dir != DATA_DIR else default_path
            try:
                loaded = import_table(tbl, path, conn)
                total_rows += loaded
            except Exception as e:
                conn.rollback()
                log_finish(conn, run_id, "failed", total_rows, str(e))
                raise

        log_finish(conn, run_id, "success", total_rows)

    finally:
        conn.close()
