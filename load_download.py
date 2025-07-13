#!/usr/bin/env python3
"""
Экспорт / импорт витрины dm.dm_f101_round_f в CSV.

    Экспорт:
        python f101_csv.py export --to-date 2018-01-31 \
                                  --file /tmp/f101_201801.csv

    Импорт (например, после ручного правления CSV):
        python f101_csv.py import --file /tmp/f101_201801.csv

Параметры подключения читаются из .env:
    DB_HOST DB_PORT DB_NAME DB_USER DB_PASSWORD
Нужен psycopg2-binary  и  pandas  (для экспорта).
"""

import os
import csv
import argparse
import datetime as dt
from contextlib import closing

import pandas as pd
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# ------------------------------------------------------------------#
# 1. Подключение к БД
# ------------------------------------------------------------------#
load_dotenv()

DB_PARAMS = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME", "postgres"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

DAG_ID       = "csv_exchange"
EXPORT_TASK  = "export_f101"
IMPORT_TASK  = "import_f101"

# ------------------------------------------------------------------#
# 2. Вспомогательные функции логирования
# ------------------------------------------------------------------#
def log_start(cur, task_id: str, note: str) -> int:
    """Создать запись STARTED и вернуть run_id."""
    cur.execute(
        """
        INSERT INTO logs.etl_runs(dag_id, task_id, started_at,
                                  status, note)
        VALUES (%s, %s, clock_timestamp(), 'STARTED', %s)
        RETURNING run_id
        """,
        (DAG_ID, task_id, note),
    )
    return cur.fetchone()[0]


def log_finish(cur, run_id: int, status: str,
               rows: int | None = None, note: str | None = None) -> None:
    """Обновить run_id статусом SUCCESS / FAILED."""
    cur.execute(
        """
        UPDATE logs.etl_runs
           SET finished_at = clock_timestamp(),
               status      = %s,
               rows_loaded = COALESCE(%s, rows_loaded),
               note        = COALESCE(%s, note)
         WHERE run_id = %s
        """,
        (status, rows, note, run_id),
    )

# ------------------------------------------------------------------#
# 3. Экспорт витрины в CSV
# ------------------------------------------------------------------#
def export_f101(to_date: str, file_path: str) -> None:
    to_date_dt = dt.datetime.strptime(to_date, "%Y-%m-%d").date()

    with closing(psycopg2.connect(**DB_PARAMS)) as conn, conn.cursor() as cur:
        run_id = log_start(cur, EXPORT_TASK, note=file_path)
        try:
            query = """
                    SELECT *
                    FROM   dm.dm_f101_round_f
                    WHERE  to_date = %s
                    ORDER  BY ledger_account, characteristic
                    """
            df = pd.read_sql(query, conn, params=(to_date_dt,))

            df.to_csv(file_path, index=False, encoding="utf-8", quoting=csv.QUOTE_MINIMAL)

            log_finish(cur, run_id, status="SUCCESS", rows=len(df))
            print(f"Exported {len(df)} rows -> {file_path}")
        except Exception as exc:
            log_finish(cur, run_id, status="FAILED", note=str(exc))
            raise


# ------------------------------------------------------------------#
# 4. Импорт CSV обратно в копию витрины
# ------------------------------------------------------------------#
DDL_COPY_TABLE = """
CREATE SCHEMA IF NOT EXISTS dm;
CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f_v2
(LIKE dm.dm_f101_round_f INCLUDING ALL);
"""

def import_f101(file_path: str) -> None:
    with closing(psycopg2.connect(**DB_PARAMS)) as conn:
        conn.autocommit = True                 # ← это решает проблему
        with conn.cursor() as cur:
            run_id = log_start(cur, IMPORT_TASK, note=file_path)
            try:
                cur.execute(DDL_COPY_TABLE)
                cur.execute("TRUNCATE dm.dm_f101_round_f_v2")

                with open(file_path, "r", encoding="utf-8") as f:
                    cur.copy_expert(
                        "COPY dm.dm_f101_round_f_v2 FROM STDIN WITH CSV HEADER",
                        f,
                    )

                cur.execute("SELECT COUNT(*) FROM dm.dm_f101_round_f_v2")
                rows = cur.fetchone()[0]

                log_finish(cur, run_id, status="SUCCESS", rows=rows)
                print(f"Imported {rows} rows <- {file_path}")
            except Exception as exc: 
                log_finish(cur, run_id, status="FAILED", note=str(exc))
                raise


# ------------------------------------------------------------------#
# 5. CLI
# ------------------------------------------------------------------#
def main() -> None:
    parser = argparse.ArgumentParser(description="F101 CSV <-> PostgreSQL")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_exp = sub.add_parser("export", help="Выгрузить CSV")
    p_exp.add_argument("--to-date", required=True, help="to_date (YYYY-MM-DD)")
    p_exp.add_argument("--file",    required=True, help="куда писать CSV")

    p_imp = sub.add_parser("import", help="Загрузить CSV")
    p_imp.add_argument("--file", required=True, help="откуда читать CSV")

    args = parser.parse_args()

    if args.cmd == "export":
        export_f101(args.to_date, args.file)
    else:
        import_f101(args.file)


if __name__ == "__main__":
    main()
