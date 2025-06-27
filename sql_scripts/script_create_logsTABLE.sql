CREATE SCHEMA IF NOT EXISTS LOGS;

CREATE TABLE IF NOT EXISTS logs.etl_runs
( run_id        BIGSERIAL PRIMARY KEY
, dag_id        TEXT        NOT NULL
, task_id       TEXT        NOT NULL
, started_at    TIMESTAMPTZ NOT NULL DEFAULT now()
, finished_at   TIMESTAMPTZ
, status        TEXT
, rows_loaded   BIGINT
, note          TEXT
);