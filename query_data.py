import pyodbc
import pandas as pd
from datetime import datetime, timedelta
import os

# Kết nối đến SQL Server
def get_connection():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};SERVER=10.91.115.154;DATABASE=master_dds;UID=gateway;PWD=gateway@mbs123ABC"
    )

# Đọc file .sql và thay thế các biến {{}} bằng context thực tế
def load_sql_template(file_path: str, context: dict) -> str:
    with open(file_path, 'r', encoding='utf-8') as f:
        template = f.read()
    for key, val in context.items():
        template = template.replace(f"{{{{{key}}}}}", val)
    return template

# Sinh tự động các mốc thời gian cho train & predict
def get_auto_date_ranges():
    today = datetime.today()
    predict_year = today.year
    predict_month = today.month

    predict_month_start = datetime(predict_year, predict_month, 1)
    label_month_date = predict_month_start - timedelta(days=1)
    train_label_month = label_month_date.strftime('%Y-%m-01')

    train_start = (label_month_date.replace(day=1) - timedelta(days=365)).strftime('%Y-%m-01')
    train_end = label_month_date.strftime('%Y-%m-%d')

    predict_start = (predict_month_start - timedelta(days=365)).strftime('%Y-%m-01')
    predict_end = (predict_month_start - timedelta(days=1)).strftime('%Y-%m-%d')

    return {
        "train_start": train_start,
        "train_end": train_end,
        "train_label_month": train_label_month,
        "predict_start": predict_start,
        "predict_end": predict_end
    }

def execute_sql_with_temp_tables(sql: str, conn) -> pd.DataFrame:
    cursor = conn.cursor()

    # Chạy từng statement trước SELECT cuối
    statements = sql.strip().split(';')
    for stmt in statements:
        stmt = stmt.strip()
        if not stmt:
            continue
        try:
            cursor.execute(stmt)
        except Exception as e:
            print(f"❌ Lỗi khi chạy câu: {stmt[:100]}...\n{e}")
            raise

    # Sau khi chạy xong, fetch kết quả từ bảng tạm
    try:
        cursor.execute("SELECT * FROM #final_result")
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame.from_records(rows, columns=columns)
    except Exception as e:
        print("❌ Không thể SELECT * FROM #final_result:", e)
        raise


# Query dữ liệu train
def query_train_data() -> pd.DataFrame:
    dates = get_auto_date_ranges()
    sql_path = os.path.join(os.path.dirname(__file__), 'sql', 'train_query.sql')
    sql = load_sql_template(sql_path, dates)
    with get_connection() as conn:
        return execute_sql_with_temp_tables(sql, conn)

# Query dữ liệu dự đoán
def query_predict_data() -> pd.DataFrame:
    dates = get_auto_date_ranges()
    sql_path = os.path.join(os.path.dirname(__file__), 'sql', 'predict_query.sql')
    sql = load_sql_template(sql_path, dates)
    with get_connection() as conn:
        return execute_sql_with_temp_tables(sql, conn)
