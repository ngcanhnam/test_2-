import pandas as pd
import pyodbc

# Đọc CSV chứa master_account
df = pd.read_csv('master_account.csv')
accounts = df['master_account'].astype(str).tolist()

# Tạo chuỗi IN (...) để nhét vào SQL
accounts_sql = ",".join(f"'{acc}'" for acc in accounts)

# Tạo query (gộp rút gọn)
query = f"""
WITH customer_daily_summary AS (
    SELECT 
        c.master_account,
        MAX(c.nav) AS max_nav,
        MAX(c.margin_total) AS max_margin,
        SUM(c.total_trading_amount) AS total_trading_amount,
        AVG(c.total_trading_amount) AS avg_total_trading_amount,
        AVG(CAST(c.loan_total AS FLOAT)) AS avg_loan,
        CASE 
            WHEN SUM(CASE WHEN FORMAT(c.report_date, 'yyyyMM') = '202503' AND c.is_login = 1 THEN 1 ELSE 0 END) > 0 THEN 1
            ELSE 0
        END AS is_login,
        MIN(c.open_date) AS open_date,
        MIN(c.first_trading_date) AS first_trading_date
    FROM fact.fact_customer_daily c
    WHERE c.report_date >= '2023-10-01'
      AND c.master_account IN ({accounts_sql})
    GROUP BY c.master_account
),

trading_summary AS (
    SELECT 
        LEFT(t.account_code, 6) AS master_account,
        MAX(t.trading_date) AS last_trading_date,
        SUM(CASE WHEN FORMAT(t.trading_date, 'yyyyMM') = '202501' THEN 1 ELSE 0 END) AS trading_days_jan,
        SUM(CASE WHEN FORMAT(t.trading_date, 'yyyyMM') = '202502' THEN 1 ELSE 0 END) AS trading_days_feb,
        SUM(CASE WHEN FORMAT(t.trading_date, 'yyyyMM') = '202503' THEN 1 ELSE 0 END) AS trading_days_mar
    FROM fact.fact_trading_mbs t
    WHERE t.trading_date >= '2023-10-01'
      AND LEFT(t.account_code, 6) IN ({accounts_sql})
    GROUP BY LEFT(t.account_code, 6)
),

margin_summary AS (
    SELECT 
        m.master_account,
        MAX(m.report_date) AS last_margin_date,
        MAX(m.rate_type) AS rate_type
    FROM fact.fact_margin_balance m
    WHERE m.report_date >= '2023-10-01'
      AND m.master_account IN ({accounts_sql})
    GROUP BY m.master_account
)

SELECT 
    cd.master_account,
    cd.max_nav,
    cd.max_margin,
    cd.total_trading_amount,
    cd.avg_total_trading_amount,
    cd.avg_loan,
    cd.is_login,
    cd.open_date,
    cd.first_trading_date,
    tr.last_trading_date,
    tr.trading_days_jan,
    tr.trading_days_feb,
    tr.trading_days_mar,
    mb.last_margin_date,
    mb.rate_type
FROM customer_daily_summary cd
LEFT JOIN trading_summary tr ON cd.master_account = tr.master_account
LEFT JOIN margin_summary mb ON cd.master_account = mb.master_account;
"""

# Kết nối SQL Server
conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=YOUR_SERVER_NAME;"
    "Database=YOUR_DATABASE_NAME;"
    "Trusted_Connection=yes;"  # hoặc dùng UID, PWD nếu cần
)

# Chạy query và lấy kết quả
result_df = pd.read_sql(query, conn)

# Xuất ra CSV nếu muốn
result_df.to_csv("result_output.csv", index=False)

# In thử 5 dòng
print(result_df.head())
