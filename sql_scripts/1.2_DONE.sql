/*--------------------------------------------------------------------
-- 1. Подготовка: схемы и витрины DM
--------------------------------------------------------------------*/
CREATE SCHEMA IF NOT EXISTS dm;

----------------------------------------------------------------------
-- DM_ACCOUNT_TURNOVER_F  – обороты по лицевым счетам
----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dm.dm_account_turnover_f
(
    on_date            DATE        NOT NULL,
    account_rk         BIGINT      NOT NULL,
    credit_amount      NUMERIC(18,2) NOT NULL DEFAULT 0,
    credit_amount_rub  NUMERIC(18,2) NOT NULL DEFAULT 0,
    debet_amount       NUMERIC(18,2) NOT NULL DEFAULT 0,
    debet_amount_rub   NUMERIC(18,2) NOT NULL DEFAULT 0,
    CONSTRAINT dm_account_turnover_pk
        PRIMARY KEY (on_date, account_rk)
);

----------------------------------------------------------------------
-- DM_ACCOUNT_BALANCE_F  – остатки по лицевым счетам
----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dm.dm_account_balance_f
(
    on_date           DATE          NOT NULL,
    account_rk        BIGINT        NOT NULL,
    balance_out       NUMERIC(18,2) NOT NULL DEFAULT 0,
    balance_out_rub   NUMERIC(18,2) NOT NULL DEFAULT 0,
    CONSTRAINT dm_account_balance_pk
        PRIMARY KEY (on_date, account_rk)
);

/*--------------------------------------------------------------------
-- 2. Процедура расчёта витрины оборотов
--------------------------------------------------------------------*/
CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_run_id        BIGINT;
    v_started_at    TIMESTAMP := clock_timestamp();
BEGIN
    /*— логируем старт —*/
    INSERT INTO logs.etl_runs(dag_id, task_id, started_at, status, note)
    VALUES ('fill_dm', 'dm_account_turnover', v_started_at, 'STARTED',
            format('on_date=%s', i_OnDate))
    RETURNING run_id INTO v_run_id;

    /*— перезагрузка за дату —*/
    DELETE FROM dm.dm_account_turnover_f
    WHERE on_date = i_OnDate;

    /*— сам расчёт —*/
    INSERT INTO dm.dm_account_turnover_f
        (on_date, account_rk,
         credit_amount, credit_amount_rub,
         debet_amount,  debet_amount_rub)
    SELECT
        i_OnDate,
        t.account_rk,
        t.credit_amount,
        t.credit_amount * COALESCE(e.reduced_cource, 1)  AS credit_amount_rub,
        t.debet_amount,
        t.debet_amount  * COALESCE(e.reduced_cource, 1)  AS debet_amount_rub
    FROM (
        /* собираем суммы по каждой стороне проводки */
        SELECT account_rk,
               SUM(credit_amount) AS credit_amount,
               SUM(debet_amount)  AS debet_amount
        FROM (
            SELECT credit_account_rk AS account_rk,
                   credit_amount,
                   0               AS debet_amount
            FROM   ds.ft_posting_f
            WHERE  oper_date = i_OnDate

            UNION ALL

            SELECT debet_account_rk  AS account_rk,
                   0,
                   debet_amount
            FROM   ds.ft_posting_f
            WHERE  oper_date = i_OnDate
        ) u
        GROUP BY account_rk
    ) t
    /* определяем валюту счёта, берём курс на дату */
    JOIN ds.md_account_d a      ON a.account_rk = t.account_rk
                               AND i_OnDate BETWEEN a.data_actual_date
                                               AND COALESCE(a.data_actual_end_date, '9999-12-31')
    LEFT JOIN LATERAL (
        SELECT reduced_cource
        FROM   ds.md_exchange_rate_d r
        WHERE  r.currency_rk = a.currency_rk
          AND  i_OnDate BETWEEN r.data_actual_date
                            AND COALESCE(r.data_actual_end_date, '9999-12-31')
        ORDER  BY r.data_actual_date DESC
        LIMIT  1
    ) e ON true;

    /*— логируем финиш —*/
    UPDATE logs.etl_runs
    SET finished_at = clock_timestamp(),
        status      = 'success',
        rows_loaded = (SELECT COUNT(*)
                       FROM dm.dm_account_turnover_f
                       WHERE on_date = i_OnDate)
    WHERE run_id = v_run_id;

EXCEPTION WHEN OTHERS THEN
    UPDATE logs.etl_runs
    SET finished_at = clock_timestamp(),
        status      = 'failed',
        note        = SQLERRM
    WHERE run_id = v_run_id;
    RAISE;
END;
$$;

----------------------------------------------------------------------
-- 3. Начальная инициализация витрины остатков на 31-12-2017
----------------------------------------------------------------------
INSERT INTO dm.dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
SELECT
    f.on_date,
    f.account_rk,
    f.balance_out,
    f.balance_out * COALESCE(e.reduced_cource, 1)
FROM ds.ft_balance_f           f
JOIN ds.md_account_d           a ON a.account_rk = f.account_rk
LEFT JOIN LATERAL (
    SELECT reduced_cource
    FROM   ds.md_exchange_rate_d r
    WHERE  r.currency_rk = a.currency_rk
      AND  DATE '2017-12-31' BETWEEN r.data_actual_date
                                 AND COALESCE(r.data_actual_end_date,'9999-12-31')
    ORDER  BY r.data_actual_date DESC
    LIMIT  1
) e ON true
WHERE f.on_date = DATE '2017-12-31'
ON CONFLICT (on_date, account_rk) DO NOTHING;

/*--------------------------------------------------------------------
-- 4. Процедура расчёта витрины остатков
--------------------------------------------------------------------*/
CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_run_id        BIGINT;
    v_started_at    TIMESTAMP := clock_timestamp();
BEGIN
    /*— логируем старт —*/
    INSERT INTO logs.etl_runs(dag_id, task_id, started_at, status, note)
    VALUES ('fill_dm', 'dm_account_balance', v_started_at, 'STARTED',
            format('on_date=%s', i_OnDate))
    RETURNING run_id INTO v_run_id;

    /*— перезагрузка за дату —*/
    DELETE FROM dm.dm_account_balance_f
    WHERE on_date = i_OnDate;

    /*— расчёт остатков —*/
    WITH acct AS (
        /* все действующие счета на дату расчёта */
        SELECT account_rk,
               char_type,
               currency_rk
        FROM   ds.md_account_d
        WHERE  i_OnDate BETWEEN data_actual_date
                           AND COALESCE(data_actual_end_date, '9999-12-31')
    ),
    prev AS (
        /* остаток на предыдущий день */
        SELECT account_rk,
               balance_out,
               balance_out_rub
        FROM   dm.dm_account_balance_f
        WHERE  on_date = i_OnDate - INTERVAL '1 day'
    ),
    turn AS (
        /* обороты за дату расчёта */
        SELECT account_rk,
               credit_amount,
               credit_amount_rub,
               debet_amount,
               debet_amount_rub
        FROM   dm.dm_account_turnover_f
        WHERE  on_date = i_OnDate
    )
    INSERT INTO dm.dm_account_balance_f
        (on_date, account_rk, balance_out, balance_out_rub)
    SELECT
        i_OnDate,
        a.account_rk,

        /* вычисляем баланс в валюте счёта */
        CASE a.char_type
             WHEN 'А' THEN
                  COALESCE(p.balance_out, 0)
                + COALESCE(t.debet_amount , 0)
                - COALESCE(t.credit_amount, 0)
             ELSE            -- 'П'
                  COALESCE(p.balance_out, 0)
                - COALESCE(t.debet_amount , 0)
                + COALESCE(t.credit_amount, 0)
        END AS balance_out,

        /* вычисляем баланс в рублях */
        CASE a.char_type
             WHEN 'А' THEN
                  COALESCE(p.balance_out_rub, 0)
                + COALESCE(t.debet_amount_rub , 0)
                - COALESCE(t.credit_amount_rub, 0)
             ELSE
                  COALESCE(p.balance_out_rub, 0)
                - COALESCE(t.debet_amount_rub , 0)
                + COALESCE(t.credit_amount_rub, 0)
        END AS balance_out_rub
    FROM acct a
    LEFT JOIN prev p  USING (account_rk)
    LEFT JOIN turn t  USING (account_rk);

    /*— логируем финиш —*/
    UPDATE logs.etl_runs
    SET finished_at = clock_timestamp(),
        status      = 'success',
        rows_loaded = (SELECT COUNT(*)
                       FROM dm.dm_account_balance_f
                       WHERE on_date = i_OnDate)
    WHERE run_id = v_run_id;

EXCEPTION WHEN OTHERS THEN
    UPDATE logs.etl_runs
    SET finished_at = clock_timestamp(),
        status      = 'failed',
        note        = SQLERRM
    WHERE run_id = v_run_id;
    RAISE;
END;
$$;

/*--------------------------------------------------------------------
-- 5. Расчёт витрин за весь январь 2018 года
--------------------------------------------------------------------*/
DO $$
DECLARE
    d DATE := DATE '2018-01-01';
BEGIN
    WHILE d <= DATE '2018-01-31' LOOP
        CALL ds.fill_account_turnover_f(d);
        CALL ds.fill_account_balance_f(d);
        d := d + INTERVAL '1 day';
    END LOOP;
END;
$$;
