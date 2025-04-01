-- Bảng này tương đương CTE master_account_data
IF OBJECT_ID('tempdb..#master_account_data') IS NOT NULL
    DROP TABLE #master_account_data;

SELECT
    LEFT(account_code, 6) AS master_account,
    MAX(trading_date)    AS last_trading_date
INTO #master_account_data
FROM master_dds.fact.fact_trading_mbs
WHERE trading_date BETWEEN '{{train_start}}' AND '{{train_end}}'
GROUP BY LEFT(account_code, 6);
-- Tương đương CTE customer_open
IF OBJECT_ID('tempdb..#customer_open') IS NOT NULL
    DROP TABLE #customer_open;

SELECT
    master_account,
    MIN(open_date) AS open_date,
    COALESCE(
      DATEDIFF(YEAR, MIN(open_date), '{{train_end}}')
      + (DATEDIFF(MONTH, MIN(open_date), '{{train_end}}')
         - DATEDIFF(YEAR, MIN(open_date), '{{train_end}}')*12)/12.0,
      0.0
    ) AS open_time
INTO #customer_open
FROM master_dds.fact.fact_customer_daily
GROUP BY master_account;
-- Tương đương CTE jan2025
IF OBJECT_ID('tempdb..#jan2025') IS NOT NULL
    DROP TABLE #jan2025;

SELECT DISTINCT
    LEFT(account_code, 6) AS master_account
INTO #jan2025
FROM master_dds.fact.fact_trading_mbs
WHERE trading_date BETWEEN '{{train_label_month}}' AND EOMONTH('{{train_label_month}}');
-- Tương đương CTE trading_freq_monthly
IF OBJECT_ID('tempdb..#trading_freq_monthly') IS NOT NULL
    DROP TABLE #trading_freq_monthly;

SELECT
    LEFT(account_code, 6)                    AS master_account,
    DATEPART(MONTH, trading_date)            AS month,
    ROUND(
       COUNT(DISTINCT trading_date) * 1.0 /
       CASE DATEPART(MONTH, trading_date)
         WHEN 1  THEN 17.0
         WHEN 2  THEN 20.0
         WHEN 3  THEN 21.0
         WHEN 4  THEN 19.0
         WHEN 5  THEN 22.0
         WHEN 6  THEN 20.0
         WHEN 7  THEN 23.0
         WHEN 8  THEN 22.0
         WHEN 9  THEN 19.0
         WHEN 10 THEN 23.0
         WHEN 11 THEN 21.0
         WHEN 12 THEN 22.0
       END,
       2
    ) AS trading_days
INTO #trading_freq_monthly
FROM master_dds.fact.fact_trading_mbs
WHERE trading_date BETWEEN '{{train_start}}' AND '{{train_end}}'
GROUP BY
    LEFT(account_code, 6),
    DATEPART(MONTH, trading_date);
-- Tương đương CTE customer_daily_monthly
IF OBJECT_ID('tempdb..#customer_daily_monthly') IS NOT NULL
    DROP TABLE #customer_daily_monthly;

SELECT
    master_account,
    DATEPART(MONTH, report_date) AS month,
    SUM(total_trading_amount) AS total_trading_amount,
    ROUND(AVG(nav), 2)                AS avg_nav,
    ROUND(AVG(loan_total), 2)                AS avg_loan_total,
    ROUND(
       (SUM(total_cash_in) - SUM(total_cash_out))
       / NULLIF(AVG(nav), 0),
       2
    ) AS cash_slow,
    COUNT(CASE WHEN is_login = 1 THEN 1 END) AS login_count,
    SUM(total_cash_in)  AS cash_in,
    SUM(total_cash_out) AS cash_out
INTO #customer_daily_monthly
FROM master_dds.fact.fact_customer_daily
WHERE report_date BETWEEN '{{train_start}}' AND '{{train_end}}'
GROUP BY
    master_account,
    DATEPART(MONTH, report_date);
-- Tương đương CTE customer_daily_agg
IF OBJECT_ID('tempdb..#customer_daily_agg') IS NOT NULL
    DROP TABLE #customer_daily_agg;

SELECT
    master_account,
    month,
    total_trading_amount,
    avg_nav,
    cash_slow,
    login_count,
    cash_in,
    cash_out,
    avg_loan_total,
    ROUND(
      (cash_in - cash_out) / NULLIF(avg_nav, 0),
      2
    ) AS cash_flow
INTO #customer_daily_agg
FROM #customer_daily_monthly;
-- Tương đương CTE share_account_monthly
IF OBJECT_ID('tempdb..#share_account_monthly') IS NOT NULL
    DROP TABLE #share_account_monthly;

SELECT
    LEFT(account_code, 6)            AS master_account,
    DATEPART(MONTH, trading_date)    AS month,
    ROUND(AVG(profit_loss_value), 2) AS avg_profit_loss_value
INTO #share_account_monthly
FROM master_dds.fact.fact_share_account_realize
WHERE trading_date BETWEEN '{{train_start}}' AND '{{train_end}}'
GROUP BY
    LEFT(account_code, 6),
    DATEPART(MONTH, trading_date);
-- Tương đương CTE order_monthly
IF OBJECT_ID('tempdb..#order_monthly') IS NOT NULL
    DROP TABLE #order_monthly;

SELECT
    LEFT(c_account_code, 6)        AS master_account,
    DATEPART(MONTH, c_order_date)  AS month,
    COUNT(DISTINCT od.c_order_no)  AS order_quantity
INTO #order_monthly
FROM DWH_TRADING.BACK.T_BACK_ORDER_HISTORY od
WHERE c_order_date BETWEEN '{{train_start}}' AND '{{train_end}}'
GROUP BY
    LEFT(c_account_code, 6),
    DATEPART(MONTH, c_order_date);
-- Tương đương CTE loan_monthly
IF OBJECT_ID('tempdb..#loan_monthly') IS NOT NULL
    DROP TABLE #loan_monthly;

SELECT
    t.master_account,
    DATEPART(MONTH, report_date) AS month,
    COUNT(DISTINCT loan_code)    AS loan_quantity
INTO #loan_monthly
FROM
(
    SELECT master_account, loan_code, report_date
    FROM master_dds.fact.fact_margin_balance
    WHERE report_date BETWEEN '{{train_start}}' AND '{{train_end}}'

    UNION ALL

    SELECT master_account, loan_code, report_date
    FROM master_dds.fact.fact_mblink_balance
    WHERE report_date BETWEEN '{{train_start}}' AND '{{train_end}}'

    UNION ALL

    SELECT master_account, loan_code, report_date
    FROM master_dds.fact.fact_adv_balance
    WHERE report_date BETWEEN '{{train_start}}' AND '{{train_end}}'
) t
GROUP BY
    t.master_account,
    DATEPART(MONTH, report_date);

IF OBJECT_ID('tempdb..#final_result') IS NOT NULL
    DROP TABLE #final_result;

SELECT *
INTO #final_result
FROM (
    SELECT
        mad.master_account,
        mad.last_trading_date,
        DATEDIFF(DAY, mad.last_trading_date, '{{predict_end}}') AS last_trading_time,
        co.open_time,
        CASE WHEN j.master_account IS NULL THEN 1 ELSE 0 END AS is_inactive,
        COALESCE(dc.age, 0)           AS age,
        COALESCE(dc.gender, 'unknown')      AS gender,
        COALESCE(dc.branch_code, 'unknown') AS branch_code,
        COALESCE(dc.channel, 'unknown')     AS channel,
        CASE 
            WHEN dc.customer_category = 'KDS' THEN 1
            WHEN dc.customer_category = 'SSG' THEN 0
            ELSE 9
        END AS customer_category,

        -- Pivot trading_days (1->12)
        MAX(CASE WHEN tf.month = 1  THEN tf.trading_days END) AS trading_days_1,
        MAX(CASE WHEN tf.month = 2  THEN tf.trading_days END) AS trading_days_2,
        MAX(CASE WHEN tf.month = 3  THEN tf.trading_days END) AS trading_days_3,
        MAX(CASE WHEN tf.month = 4  THEN tf.trading_days END) AS trading_days_4,
        MAX(CASE WHEN tf.month = 5  THEN tf.trading_days END) AS trading_days_5,
        MAX(CASE WHEN tf.month = 6  THEN tf.trading_days END) AS trading_days_6,
        MAX(CASE WHEN tf.month = 7  THEN tf.trading_days END) AS trading_days_7,
        MAX(CASE WHEN tf.month = 8  THEN tf.trading_days END) AS trading_days_8,
        MAX(CASE WHEN tf.month = 9  THEN tf.trading_days END) AS trading_days_9,
        MAX(CASE WHEN tf.month = 10 THEN tf.trading_days END) AS trading_days_10,
        MAX(CASE WHEN tf.month = 11 THEN tf.trading_days END) AS trading_days_11,
        MAX(CASE WHEN tf.month = 12 THEN tf.trading_days END) AS trading_days_12,

        -- Pivot total_trading_amount (1->12)
        MAX(CASE WHEN cda.month = 1  THEN cda.total_trading_amount END) AS total_trading_amount_1,
        MAX(CASE WHEN cda.month = 2  THEN cda.total_trading_amount END) AS total_trading_amount_2,
        MAX(CASE WHEN cda.month = 3  THEN cda.total_trading_amount END) AS total_trading_amount_3,
        MAX(CASE WHEN cda.month = 4  THEN cda.total_trading_amount END) AS total_trading_amount_4,
        MAX(CASE WHEN cda.month = 5  THEN cda.total_trading_amount END) AS total_trading_amount_5,
        MAX(CASE WHEN cda.month = 6  THEN cda.total_trading_amount END) AS total_trading_amount_6,
        MAX(CASE WHEN cda.month = 7  THEN cda.total_trading_amount END) AS total_trading_amount_7,
        MAX(CASE WHEN cda.month = 8  THEN cda.total_trading_amount END) AS total_trading_amount_8,
        MAX(CASE WHEN cda.month = 9  THEN cda.total_trading_amount END) AS total_trading_amount_9,
        MAX(CASE WHEN cda.month = 10 THEN cda.total_trading_amount END) AS total_trading_amount_10,
        MAX(CASE WHEN cda.month = 11 THEN cda.total_trading_amount END) AS total_trading_amount_11,
        MAX(CASE WHEN cda.month = 12 THEN cda.total_trading_amount END) AS total_trading_amount_12,

        -- Pivot avg_nav (1->12)
        MAX(CASE WHEN cda.month = 1  THEN cda.avg_nav END) AS avg_nav_1,
        MAX(CASE WHEN cda.month = 2  THEN cda.avg_nav END) AS avg_nav_2,
        MAX(CASE WHEN cda.month = 3  THEN cda.avg_nav END) AS avg_nav_3,
        MAX(CASE WHEN cda.month = 4  THEN cda.avg_nav END) AS avg_nav_4,
        MAX(CASE WHEN cda.month = 5  THEN cda.avg_nav END) AS avg_nav_5,
        MAX(CASE WHEN cda.month = 6  THEN cda.avg_nav END) AS avg_nav_6,
        MAX(CASE WHEN cda.month = 7  THEN cda.avg_nav END) AS avg_nav_7,
        MAX(CASE WHEN cda.month = 8  THEN cda.avg_nav END) AS avg_nav_8,
        MAX(CASE WHEN cda.month = 9  THEN cda.avg_nav END) AS avg_nav_9,
        MAX(CASE WHEN cda.month = 10 THEN cda.avg_nav END) AS avg_nav_10,
        MAX(CASE WHEN cda.month = 11 THEN cda.avg_nav END) AS avg_nav_11,
        MAX(CASE WHEN cda.month = 12 THEN cda.avg_nav END) AS avg_nav_12,
        

        -- Pivot login_count (1->12)
        MAX(CASE WHEN cda.month = 1  THEN cda.login_count END) AS login_count_1,
        MAX(CASE WHEN cda.month = 2  THEN cda.login_count END) AS login_count_2,
        MAX(CASE WHEN cda.month = 3  THEN cda.login_count END) AS login_count_3,
        MAX(CASE WHEN cda.month = 4  THEN cda.login_count END) AS login_count_4,
        MAX(CASE WHEN cda.month = 5  THEN cda.login_count END) AS login_count_5,
        MAX(CASE WHEN cda.month = 6  THEN cda.login_count END) AS login_count_6,
        MAX(CASE WHEN cda.month = 7  THEN cda.login_count END) AS login_count_7,
        MAX(CASE WHEN cda.month = 8  THEN cda.login_count END) AS login_count_8,
        MAX(CASE WHEN cda.month = 9  THEN cda.login_count END) AS login_count_9,
        MAX(CASE WHEN cda.month = 10 THEN cda.login_count END) AS login_count_10,
        MAX(CASE WHEN cda.month = 11 THEN cda.login_count END) AS login_count_11,
        MAX(CASE WHEN cda.month = 12 THEN cda.login_count END) AS login_count_12,

        -- Pivot cash_flow (1->12)
        MAX(CASE WHEN cda.month = 1  THEN cda.cash_flow END) AS cash_flow_1,
        MAX(CASE WHEN cda.month = 2  THEN cda.cash_flow END) AS cash_flow_2,
        MAX(CASE WHEN cda.month = 3  THEN cda.cash_flow END) AS cash_flow_3,
        MAX(CASE WHEN cda.month = 4  THEN cda.cash_flow END) AS cash_flow_4,
        MAX(CASE WHEN cda.month = 5  THEN cda.cash_flow END) AS cash_flow_5,
        MAX(CASE WHEN cda.month = 6  THEN cda.cash_flow END) AS cash_flow_6,
        MAX(CASE WHEN cda.month = 7  THEN cda.cash_flow END) AS cash_flow_7,
        MAX(CASE WHEN cda.month = 8  THEN cda.cash_flow END) AS cash_flow_8,
        MAX(CASE WHEN cda.month = 9  THEN cda.cash_flow END) AS cash_flow_9,
        MAX(CASE WHEN cda.month = 10 THEN cda.cash_flow END) AS cash_flow_10,
        MAX(CASE WHEN cda.month = 11 THEN cda.cash_flow END) AS cash_flow_11,
        MAX(CASE WHEN cda.month = 12 THEN cda.cash_flow END) AS cash_flow_12,

        -- Pivot cash_in (1->12)
        MAX(CASE WHEN cda.month = 1  THEN cda.cash_in END) AS cash_in_1,
        MAX(CASE WHEN cda.month = 2  THEN cda.cash_in END) AS cash_in_2,
        MAX(CASE WHEN cda.month = 3  THEN cda.cash_in END) AS cash_in_3,
        MAX(CASE WHEN cda.month = 4  THEN cda.cash_in END) AS cash_in_4,
        MAX(CASE WHEN cda.month = 5  THEN cda.cash_in END) AS cash_in_5,
        MAX(CASE WHEN cda.month = 6  THEN cda.cash_in END) AS cash_in_6,
        MAX(CASE WHEN cda.month = 7  THEN cda.cash_in END) AS cash_in_7,
        MAX(CASE WHEN cda.month = 8  THEN cda.cash_in END) AS cash_in_8,
        MAX(CASE WHEN cda.month = 9  THEN cda.cash_in END) AS cash_in_9,
        MAX(CASE WHEN cda.month = 10 THEN cda.cash_in END) AS cash_in_10,
        MAX(CASE WHEN cda.month = 11 THEN cda.cash_in END) AS cash_in_11,
        MAX(CASE WHEN cda.month = 12 THEN cda.cash_in END) AS cash_in_12,

        -- Pivot cash_out (1->12)
        MAX(CASE WHEN cda.month = 1  THEN cda.cash_out END) AS cash_out_1,
        MAX(CASE WHEN cda.month = 2  THEN cda.cash_out END) AS cash_out_2,
        MAX(CASE WHEN cda.month = 3  THEN cda.cash_out END) AS cash_out_3,
        MAX(CASE WHEN cda.month = 4  THEN cda.cash_out END) AS cash_out_4,
        MAX(CASE WHEN cda.month = 5  THEN cda.cash_out END) AS cash_out_5,
        MAX(CASE WHEN cda.month = 6  THEN cda.cash_out END) AS cash_out_6,
        MAX(CASE WHEN cda.month = 7  THEN cda.cash_out END) AS cash_out_7,
        MAX(CASE WHEN cda.month = 8  THEN cda.cash_out END) AS cash_out_8,
        MAX(CASE WHEN cda.month = 9  THEN cda.cash_out END) AS cash_out_9,
        MAX(CASE WHEN cda.month = 10 THEN cda.cash_out END) AS cash_out_10,
        MAX(CASE WHEN cda.month = 11 THEN cda.cash_out END) AS cash_out_11,
        MAX(CASE WHEN cda.month = 12 THEN cda.cash_out END) AS cash_out_12,

        -- Pivot loan_total (1->12)
        MAX(CASE WHEN cda.month = 1  THEN cda.avg_loan_total END) AS avg_loan_total_1,
        MAX(CASE WHEN cda.month = 2  THEN cda.avg_loan_total END) AS avg_loan_total_2,
        MAX(CASE WHEN cda.month = 3  THEN cda.avg_loan_total END) AS avg_loan_total_3,
        MAX(CASE WHEN cda.month = 4  THEN cda.avg_loan_total END) AS avg_loan_total_4,
        MAX(CASE WHEN cda.month = 5  THEN cda.avg_loan_total END) AS avg_loan_total_5,
        MAX(CASE WHEN cda.month = 6  THEN cda.avg_loan_total END) AS avg_loan_total_6,
        MAX(CASE WHEN cda.month = 7  THEN cda.avg_loan_total END) AS avg_loan_total_7,
        MAX(CASE WHEN cda.month = 8  THEN cda.avg_loan_total END) AS avg_loan_total_8,
        MAX(CASE WHEN cda.month = 9  THEN cda.avg_loan_total END) AS avg_loan_total_9,
        MAX(CASE WHEN cda.month = 10 THEN cda.avg_loan_total END) AS avg_loan_total_10,
        MAX(CASE WHEN cda.month = 11 THEN cda.avg_loan_total END) AS avg_loan_total_11,
        MAX(CASE WHEN cda.month = 12 THEN cda.avg_loan_total END) AS avg_loan_total_12,

        -- Pivot avg_profit_loss_value (1->12)
        MAX(CASE WHEN sam.month = 1  THEN sam.avg_profit_loss_value END) AS avg_profit_loss_value_1,
        MAX(CASE WHEN sam.month = 2  THEN sam.avg_profit_loss_value END) AS avg_profit_loss_value_2,
        MAX(CASE WHEN sam.month = 3  THEN sam.avg_profit_loss_value END) AS avg_profit_loss_value_3,
        MAX(CASE WHEN sam.month = 4  THEN sam.avg_profit_loss_value END) AS avg_profit_loss_value_4,
        MAX(CASE WHEN sam.month = 5  THEN sam.avg_profit_loss_value END) AS avg_profit_loss_value_5,
        MAX(CASE WHEN sam.month = 6  THEN sam.avg_profit_loss_value END) AS avg_profit_loss_value_6,
        MAX(CASE WHEN sam.month = 7  THEN sam.avg_profit_loss_value END) AS avg_profit_loss_value_7,
        MAX(CASE WHEN sam.month = 8  THEN sam.avg_profit_loss_value END) AS avg_profit_loss_value_8,
        MAX(CASE WHEN sam.month = 9  THEN sam.avg_profit_loss_value END) AS avg_profit_loss_value_9,
        MAX(CASE WHEN sam.month = 10 THEN sam.avg_profit_loss_value END) AS avg_profit_loss_value_10,
        MAX(CASE WHEN sam.month = 11 THEN sam.avg_profit_loss_value END) AS avg_profit_loss_value_11,
        MAX(CASE WHEN sam.month = 12 THEN sam.avg_profit_loss_value END) AS avg_profit_loss_value_12,

        -- Pivot order_frequency (dựa trên order_quantity / số ngày giao dịch chuẩn)
        ROUND(MAX(CASE WHEN om.month = 1  THEN om.order_quantity END) / 17.0, 2) AS order_frequency_1,
        ROUND(MAX(CASE WHEN om.month = 2  THEN om.order_quantity END) / 20.0, 2) AS order_frequency_2,
        ROUND(MAX(CASE WHEN om.month = 3  THEN om.order_quantity END) / 21.0, 2) AS order_frequency_3,
        ROUND(MAX(CASE WHEN om.month = 4  THEN om.order_quantity END) / 19.0, 2) AS order_frequency_4,
        ROUND(MAX(CASE WHEN om.month = 5  THEN om.order_quantity END) / 22.0, 2) AS order_frequency_5,
        ROUND(MAX(CASE WHEN om.month = 6  THEN om.order_quantity END) / 20.0, 2) AS order_frequency_6,
        ROUND(MAX(CASE WHEN om.month = 7  THEN om.order_quantity END) / 23.0, 2) AS order_frequency_7,
        ROUND(MAX(CASE WHEN om.month = 8  THEN om.order_quantity END) / 22.0, 2) AS order_frequency_8,
        ROUND(MAX(CASE WHEN om.month = 9  THEN om.order_quantity END) / 19.0, 2) AS order_frequency_9,
        ROUND(MAX(CASE WHEN om.month = 10 THEN om.order_quantity END) / 23.0, 2) AS order_frequency_10,
        ROUND(MAX(CASE WHEN om.month = 11 THEN om.order_quantity END) / 21.0, 2) AS order_frequency_11,
        ROUND(MAX(CASE WHEN om.month = 12 THEN om.order_quantity END) / 22.0, 2) AS order_frequency_12,

        -- Pivot loan_quantity (giữ nguyên)
        MAX(CASE WHEN lm.month = 1  THEN lm.loan_quantity END) AS loan_quantity_1,
        MAX(CASE WHEN lm.month = 2  THEN lm.loan_quantity END) AS loan_quantity_2,
        MAX(CASE WHEN lm.month = 3  THEN lm.loan_quantity END) AS loan_quantity_3,
        MAX(CASE WHEN lm.month = 4  THEN lm.loan_quantity END) AS loan_quantity_4,
        MAX(CASE WHEN lm.month = 5  THEN lm.loan_quantity END) AS loan_quantity_5,
        MAX(CASE WHEN lm.month = 6  THEN lm.loan_quantity END) AS loan_quantity_6,
        MAX(CASE WHEN lm.month = 7  THEN lm.loan_quantity END) AS loan_quantity_7,
        MAX(CASE WHEN lm.month = 8  THEN lm.loan_quantity END) AS loan_quantity_8,
        MAX(CASE WHEN lm.month = 9  THEN lm.loan_quantity END) AS loan_quantity_9,
        MAX(CASE WHEN lm.month = 10 THEN lm.loan_quantity END) AS loan_quantity_10,
        MAX(CASE WHEN lm.month = 11 THEN lm.loan_quantity END) AS loan_quantity_11,
        MAX(CASE WHEN lm.month = 12 THEN lm.loan_quantity END) AS loan_quantity_12

    FROM #master_account_data          AS mad
    LEFT JOIN #jan2025                 AS j   ON mad.master_account = j.master_account
    LEFT JOIN #customer_open           AS co  ON mad.master_account = co.master_account
    LEFT JOIN master_dds.dim.dim_customer dc   ON dc.master_account = mad.master_account
    LEFT JOIN #trading_freq_monthly    AS tf  ON mad.master_account = tf.master_account
    LEFT JOIN #customer_daily_agg      AS cda ON mad.master_account = cda.master_account
    LEFT JOIN #share_account_monthly   AS sam ON mad.master_account = sam.master_account
    LEFT JOIN #order_monthly           AS om  ON mad.master_account = om.master_account
    LEFT JOIN #loan_monthly            AS lm  ON mad.master_account = lm.master_account
    GROUP BY
        mad.master_account,
        mad.last_trading_date,
        co.open_time,
        j.master_account,
        dc.age,
        dc.gender,
        dc.branch_code,
        dc.channel,
        dc.customer_category
) AS raw;

SELECT * FROM #final_result;
