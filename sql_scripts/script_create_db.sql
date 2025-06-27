CREATE SCHEMA IF NOT EXISTS DS;

CREATE TABLE IF NOT EXISTS ds.ft_balance_f
( on_date      DATE    NOT NULL
, account_rk   BIGINT  NOT NULL      
, currency_rk  BIGINT
, balance_out  DOUBLE PRECISION      -- FLOAT
, PRIMARY KEY (on_date, account_rk)
);

CREATE TABLE IF NOT EXISTS ds.ft_posting_f
( oper_date         DATE    NOT NULL
, credit_account_rk BIGINT  NOT NULL
, debet_account_rk  BIGINT  NOT NULL
, credit_amount     DOUBLE PRECISION
, debet_amount      DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS ds.md_account_d
( data_actual_date      DATE    NOT NULL
, data_actual_end_date  DATE    NOT NULL
, account_rk            BIGINT  NOT NULL
, account_number        VARCHAR(20) NOT NULL
, char_type             CHAR(1)     NOT NULL
, currency_rk           BIGINT  NOT NULL
, currency_code         VARCHAR(3)  NOT NULL
, PRIMARY KEY (data_actual_date, account_rk)
);

CREATE TABLE IF NOT EXISTS ds.md_currency_d
( currency_rk           BIGINT      NOT NULL
, data_actual_date      DATE        NOT NULL
, data_actual_end_date  DATE
, currency_code         VARCHAR(3)
, code_iso_char         VARCHAR(3)
, PRIMARY KEY (currency_rk, data_actual_date)
);

CREATE TABLE IF NOT EXISTS ds.md_exchange_rate_d
( data_actual_date      DATE    NOT NULL
, data_actual_end_date  DATE
, currency_rk           BIGINT  NOT NULL
, reduced_cource        DOUBLE PRECISION
, code_iso_num          VARCHAR(3)
, PRIMARY KEY (data_actual_date, currency_rk)
);

CREATE TABLE IF NOT EXISTS ds.md_ledger_account_s
( chapter                      CHAR(1)
, chapter_name                 VARCHAR(16)
, section_number               INTEGER
, section_name                 VARCHAR(22)
, subsection_name              VARCHAR(21)
, ledger1_account              INTEGER
, ledger1_account_name         VARCHAR(47)
, ledger_account               INTEGER NOT NULL
, ledger_account_name          VARCHAR(153)
, characteristic               CHAR(1)
, is_resident                  INTEGER
, is_reserve                   INTEGER
, is_reserved                  INTEGER
, is_loan                      INTEGER
, is_reserved_assets           INTEGER
, is_overdue                   INTEGER
, is_interest                  INTEGER
, pair_account                 VARCHAR(5)
, start_date                   DATE    NOT NULL
, end_date                     DATE
, is_rub_only                  INTEGER
, min_term                     VARCHAR(1)
, min_term_measure             VARCHAR(1)
, max_term                     VARCHAR(1)
, max_term_measure             VARCHAR(1)
, ledger_acc_full_name_translit VARCHAR(1)
, is_revaluation               VARCHAR(1)
, is_correct                   VARCHAR(1)
, PRIMARY KEY (ledger_account, start_date)
);
