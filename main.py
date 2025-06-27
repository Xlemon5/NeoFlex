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
CSV → PostgreSQL bulk‑loader + ETL‑логирование (v2)
===================================================

* Поддерживает upsert по первичным ключам (см. таблицу PK ниже).
* Для `ds.ft_posting_f` (нет PK) выполняет `TRUNCATE` перед загрузкой.
* Чистит дубликаты, нормализует заголовки, ISO‑даты, VARCHAR‑лимиты.
* Логи в `logs.etl_runs` (уже создана).
* Пауза 5 сек после старта лога для наглядности `started_at` → `finished_at`.

| Таблица | PK | Стратегия |
|---------|----|-----------|
| ds.ft_balance_f | on_date, account_rk | upsert |
| ds.ft_posting_f | — | truncate+insert |
| ds.md_account_d | data_actual_date, account_rk | upsert |
| ds.md_currency_d | currency_rk, data_actual_date | upsert |
| ds.md_exchange_rate_d | data_actual_date, currency_rk | upsert |
| ds.md_ledger_account_s | ledger_account, start_date | upsert |

Запуск:
```bash
python csv_to_pg_loader.py /path/to/csv_dir
```"""

# --------------------------------------------------------------------------- #
# 1. Конфигурация                                                             #
# --------------------------------------------------------------------------- #
load_dotenv()
conn_params = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

DATA_DIR = Path("/Users/ilya/Desktop/neoFlex/data")  # default CSV directory
DAG_ID, TASK_ID = "csv_loader", "bulk_load_csv"
LOG_TABLE = "logs.etl_runs"

def make_path(name):
    return DATA_DIR / f"{name}.csv"

csv_files = {
    "ft_balance_f": make_path("ft_balance_f"),
    "ft_posting_f": make_path("ft_posting_f"),
    "md_account_d": make_path("md_account_d"),
    "md_currency_d": make_path("md_currency_d"),
    "md_exchange_rate_d": make_path("md_exchange_rate_d"),
    "md_ledger_account_s": make_path("md_ledger_account_s"),
}

# --------------------------------------------------------------------------- #
# 2. INSERT / UPSERT шаблоны                                                  #
# --------------------------------------------------------------------------- #
insert_queries = {
    "ft_balance_f": (
        "INSERT INTO ds.ft_balance_f (on_date, account_rk, currency_rk, balance_out) "
        "VALUES (%s,%s,%s,%s) "
        "ON CONFLICT (on_date, account_rk) DO UPDATE SET "
        "currency_rk = EXCLUDED.currency_rk, balance_out = EXCLUDED.balance_out;"
    ),
    "md_account_d": (
        "INSERT INTO ds.md_account_d (data_actual_date, data_actual_end_date, account_rk, "
        "account_number, char_type, currency_rk, currency_code) VALUES (%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (data_actual_date, account_rk) DO UPDATE SET "
        "data_actual_end_date = EXCLUDED.data_actual_end_date, account_number = EXCLUDED.account_number, "
        "char_type = EXCLUDED.char_type, currency_rk = EXCLUDED.currency_rk, currency_code = EXCLUDED.currency_code;"
    ),
    "md_currency_d": (
        "INSERT INTO ds.md_currency_d (currency_rk, data_actual_date, data_actual_end_date, currency_code, code_iso_char) "
        "VALUES (%s,%s,%s,%s,%s) "
        "ON CONFLICT (currency_rk, data_actual_date) DO UPDATE SET "
        "data_actual_end_date = EXCLUDED.data_actual_end_date, currency_code = EXCLUDED.currency_code, code_iso_char = EXCLUDED.code_iso_char;"
    ),
    "md_exchange_rate_d": (
        "INSERT INTO ds.md_exchange_rate_d (data_actual_date, data_actual_end_date, currency_rk, reduced_cource, code_iso_num) "
        "VALUES (%s,%s,%s,%s,%s) "
        "ON CONFLICT (data_actual_date, currency_rk) DO UPDATE SET "
        "data_actual_end_date = EXCLUDED.data_actual_end_date, reduced_cource = EXCLUDED.reduced_cource, code_iso_num = EXCLUDED.code_iso_num;"
    ),
    "md_ledger_account_s": (
        "INSERT INTO ds.md_ledger_account_s (chapter, chapter_name, section_number, section_name, subsection_name, "
        "ledger1_account, ledger1_account_name, ledger_account, ledger_account_name, characteristic, start_date, end_date) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (ledger_account, start_date) DO UPDATE SET "
        "chapter = EXCLUDED.chapter, chapter_name = EXCLUDED.chapter_name, section_number = EXCLUDED.section_number, "
        "section_name = EXCLUDED.section_name, subsection_name = EXCLUDED.subsection_name, ledger1_account = EXCLUDED.ledger1_account, "
        "ledger1_account_name = EXCLUDED.ledger1_account_name, ledger_account_name = EXCLUDED.ledger_account_name, "
        "characteristic = EXCLUDED.characteristic, end_date = EXCLUDED.end_date;"
    ),
}

# таблица без PK — полная перезаливка
insert_queries["ft_posting_f"] = (
    "INSERT INTO ds.ft_posting_f (oper_date, credit_account_rk, debet_account_rk, credit_amount, debet_amount) "
    "VALUES (%s,%s,%s,%s,%s);"
)

# --------------------------------------------------------------------------- #
# 3. Даты / VARCHAR лимиты                                                    #
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

def snake_case(s: str) -> str:
    return s.strip().replace(" ", "_").replace("-", "_").lower()


def convert_date(value: str, fmt: str) -> str | None:
    if not value or str(value).lower() in {"nan", "none"}:
        return None
    try:
        if fmt == "DD.MM.YYYY":
            return datetime.strptime(value, "%d.%m.%Y").strftime("%Y-%m-%d")
        if fmt == "DD-MM-YYYY":
            return datetime.strptime(value, "%d-%m-%Y").strftime("%Y-%m-%d")
        if fmt == "YYYY-MM-DD":
            return value
    except ValueError:
        pass
    return None


def pythonify(v):
    if v is None: return None
    if isinstance(v, float) and math.isnan(v): return None
    if isinstance(v, np.generic): return v.item()
    return v

# --------------------------------------------------------------------------- #
# 5. DataFrame подготовка                                                     #
# --------------------------------------------------------------------------- #

def prepare_dataframe(table: str, path: Path) -> pd.DataFrame:
    for enc in ("utf-8", "utf-8-sig", "cp1251", "latin-1"):
        try:
            df = pd.read_csv(path, delimiter=";", encoding=enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise UnicodeDecodeError("Не удалось открыть файл", path, 0, 0, "кодировки")

    df.columns = [snake_case(c) for c in df.columns]
    dup_mask = df.duplicated(keep=False)
    if dup_mask.any():
        print(f"[WARN] {path.name}: {dup_mask.sum()} дубликат(ов) отброшено")
        df = df[~dup_mask]

    if table in date_columns:
        for col, fmt in date_columns[table].items():
            if col in df.columns:
                df[col] = df[col].apply(lambda x: convert_date(str(x), fmt))
        df = df.dropna(subset=date_columns[table].keys())

    if table in varchar_limits:
        for col, lim in varchar_limits[table].items():
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str[:lim].replace({"": None})

    return df

# --------------------------------------------------------------------------- #
# 6. Логирование                                                              #
# --------------------------------------------------------------------------- #

def log_start(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(f"INSERT INTO {LOG_TABLE} (dag_id, task_id, status) VALUES (%s,%s,'running') RETURNING run_id", (DAG_ID, TASK_ID))
        run_id = cur.fetchone()[0]
    conn.commit()
    print(f"[LOG] run {run_id} started")
    return run_id


def log_finish(conn, run_id: int, status: str, rows: int, note: str | None = None):
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {LOG_TABLE} SET finished_at = now(), status = %s, rows_loaded = %s, note = %s WHERE run_id = %s",
            (status, rows, note, run_id),
        )
    conn.commit()
    print(f"[LOG] run {run_id} finished status={status} rows={rows}")

# --------------------------------------------------------------------------- #
# 7. Загрузка одной таблицы                                                   #
# --------------------------------------------------------------------------- #

def import_table(conn, table: str, path: Path) -> int:
    """Возвращает число успешно загруженных строк."""
    if not path.exists():
        print(f"[SKIP] {path} отсутствует")
        return 0

    df = prepare_dataframe(table, path)
    if df.empty:
        print(f"[SKIP] {path.name}: после очистки строк не осталось")
        return 0

    # порядок столбцов должен соответствовать VALUES(...)
    cols_order = [snake_case(c.strip()) for c in insert_queries[table].split("(")[1].split(")")[0].split(",")]
    if missing := [c for c in cols_order if c not in df.columns]:
        raise ValueError(f"{path.name}: отсутствуют колонки {missing}")

    df = df[cols_order].where(pd.notnull(df), None)
    rows = [[pythonify(v) for v in r] for r in df.to_numpy()]

    with conn.cursor() as cur:
        if table == "ft_posting_f":
            cur.execute("TRUNCATE TABLE ds.ft_posting_f")
        cur.executemany(insert_queries[table], rows)
    print(f"[OK] {table}: {len(rows)} строк загружено")
    return len(rows)

# --------------------------------------------------------------------------- #
# 8. main                                                                     #
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    import sys
    root_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else DATA_DIR

    conn = psycopg2.connect(**conn_params)
    try:
        run_id = log_start(conn)
        time.sleep(5)  # демонстрационная пауза

        total = 0
        for tbl, default_path in csv_files.items():
            path = root_dir / default_path.name if root_dir != DATA_DIR else default_path
            try:
                total += import_table(conn, tbl, path)
            except Exception as e:
                conn.rollback()
                log_finish(conn, run_id, "failed", total, str(e))
                raise

        log_finish(conn, run_id, "success", total)
    finally:
        conn.close()
