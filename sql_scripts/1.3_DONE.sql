/*================================================================
  1. Витрина данных формы-101  (округлённый счёт 2-го порядка)
================================================================*/
CREATE SCHEMA IF NOT EXISTS dm;

CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f
(
    from_date          DATE                      NOT NULL,      -- начало периода
    to_date            DATE                      NOT NULL,      -- конец периода
    chapter            CHAR(1)                   NOT NULL,      -- глава баланса
    ledger_account     CHAR(5)                   NOT NULL,      -- счёт 2-го порядка
    characteristic     CHAR(1)                   NOT NULL,      -- 'А' / 'П'
    balance_in_rub     NUMERIC(23,8)  DEFAULT 0  NOT NULL,
    balance_in_val     NUMERIC(23,8)  DEFAULT 0  NOT NULL,
    balance_in_total   NUMERIC(23,8)  DEFAULT 0  NOT NULL,
    turn_deb_rub       NUMERIC(23,8)  DEFAULT 0  NOT NULL,
    turn_deb_val       NUMERIC(23,8)  DEFAULT 0  NOT NULL,
    turn_deb_total     NUMERIC(23,8)  DEFAULT 0  NOT NULL,
    turn_cre_rub       NUMERIC(23,8)  DEFAULT 0  NOT NULL,
    turn_cre_val       NUMERIC(23,8)  DEFAULT 0  NOT NULL,
    turn_cre_total     NUMERIC(23,8)  DEFAULT 0  NOT NULL,
    balance_out_rub    NUMERIC(23,8)  DEFAULT 0  NOT NULL,
    balance_out_val    NUMERIC(23,8)  DEFAULT 0  NOT NULL,
    balance_out_total  NUMERIC(23,8)  DEFAULT 0  NOT NULL,
    CONSTRAINT dm_f101_round_pk
        PRIMARY KEY (to_date, ledger_account, characteristic)
);

/*================================================================
  2. Процедура dm.fill_f101_round_f
================================================================*/
CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_run_id     BIGINT;
    v_started_at TIMESTAMP := clock_timestamp();

    v_from_date  DATE := date_trunc('month', i_OnDate - INTERVAL '1 day')::date;
    v_to_date    DATE := (i_OnDate - INTERVAL '1 day')::date;
    v_prev_date  DATE := v_from_date - INTERVAL '1 day';
BEGIN
    /* логируем старт */
    INSERT INTO logs.etl_runs(dag_id, task_id, started_at, status, note)
    VALUES ('fill_dm','dm_f101_round',v_started_at,'STARTED',
            format('report_on=%s (period %s-%s)',i_OnDate,v_from_date,v_to_date))
    RETURNING run_id INTO v_run_id;

    /* пересчет → чистим период */
    DELETE FROM dm.dm_f101_round_f
    WHERE from_date = v_from_date AND to_date = v_to_date;

    /* источники */
    WITH acc AS (
        SELECT DISTINCT
               a.account_rk,
               LEFT(a.account_number::text,5)                AS ledger_account,
               a.char_type                                   AS characteristic,
               ls.chapter,
               (a.currency_code::text IN ('810','643'))      AS is_rub
        FROM   ds.md_account_d a
        JOIN   ds.md_ledger_account_s ls
               ON ls.ledger_account::text = LEFT(a.account_number::text,5)
        WHERE  daterange(a.data_actual_date,
                         COALESCE(a.data_actual_end_date,'9999-12-31'::date),'[]')
           &&  daterange(v_from_date, v_to_date,'[]')
    ),
    bal_in AS (
        SELECT account_rk, balance_out_rub
        FROM   dm.dm_account_balance_f
        WHERE  on_date = v_prev_date
    ),
    bal_out AS (
        SELECT account_rk, balance_out_rub
        FROM   dm.dm_account_balance_f
        WHERE  on_date = v_to_date
    ),
    turn AS (
        SELECT account_rk,
               SUM(debet_amount_rub)  AS debet_rub,
               SUM(credit_amount_rub) AS credit_rub
        FROM   dm.dm_account_turnover_f
        WHERE  on_date BETWEEN v_from_date AND v_to_date
        GROUP  BY account_rk
    )

    /* агрегация */
    INSERT INTO dm.dm_f101_round_f
        (from_date,to_date,chapter,ledger_account,characteristic,
         balance_in_rub,balance_in_val,balance_in_total,
         turn_deb_rub,turn_deb_val,turn_deb_total,
         turn_cre_rub,turn_cre_val,turn_cre_total,
         balance_out_rub,balance_out_val,balance_out_total)
    SELECT
        v_from_date,
        v_to_date,
        a.chapter,
        a.ledger_account,
        a.characteristic,

        /* входящий остаток */
        COALESCE(SUM(CASE WHEN a.is_rub      THEN COALESCE(bi.balance_out_rub,0) END),0) AS balance_in_rub,
        COALESCE(SUM(CASE WHEN NOT a.is_rub  THEN COALESCE(bi.balance_out_rub,0) END),0) AS balance_in_val,
        COALESCE(SUM(COALESCE(bi.balance_out_rub,0)),0)                                   AS balance_in_total,

        /* дебетовые обороты */
        COALESCE(SUM(CASE WHEN a.is_rub      THEN COALESCE(t.debet_rub,0) END),0)         AS turn_deb_rub,
        COALESCE(SUM(CASE WHEN NOT a.is_rub  THEN COALESCE(t.debet_rub,0) END),0)         AS turn_deb_val,
        COALESCE(SUM(COALESCE(t.debet_rub,0)),0)                                          AS turn_deb_total,

        /* кредитовые обороты */
        COALESCE(SUM(CASE WHEN a.is_rub      THEN COALESCE(t.credit_rub,0) END),0)        AS turn_cre_rub,
        COALESCE(SUM(CASE WHEN NOT a.is_rub  THEN COALESCE(t.credit_rub,0) END),0)        AS turn_cre_val,
        COALESCE(SUM(COALESCE(t.credit_rub,0)),0)                                         AS turn_cre_total,

        /* исходящий остаток */
        COALESCE(SUM(CASE WHEN a.is_rub      THEN COALESCE(bo.balance_out_rub,0) END),0)  AS balance_out_rub,
        COALESCE(SUM(CASE WHEN NOT a.is_rub  THEN COALESCE(bo.balance_out_rub,0) END),0)  AS balance_out_val,
        COALESCE(SUM(COALESCE(bo.balance_out_rub,0)),0)                                   AS balance_out_total
    FROM   acc a
    LEFT   JOIN bal_in  bi ON bi.account_rk = a.account_rk
    LEFT   JOIN bal_out bo ON bo.account_rk = a.account_rk
    LEFT   JOIN turn    t  ON t.account_rk  = a.account_rk
    GROUP  BY a.chapter, a.ledger_account, a.characteristic;

    /* логируем успех */
    UPDATE logs.etl_runs
    SET finished_at = clock_timestamp(),
        status      = 'SUCCESS',
        rows_loaded = (SELECT COUNT(*) FROM dm.dm_f101_round_f
                       WHERE from_date = v_from_date AND to_date = v_to_date)
    WHERE run_id = v_run_id;

EXCEPTION WHEN OTHERS THEN
    UPDATE logs.etl_runs
    SET finished_at = clock_timestamp(),
        status      = 'FAILED',
        note        = SQLERRM
    WHERE run_id = v_run_id;
    RAISE;
END;
$$;


/*================================================================
  5. Одноразовый расчёт за январь-2018
================================================================*/
CALL dm.fill_f101_round_f('2018-02-01'::date);
