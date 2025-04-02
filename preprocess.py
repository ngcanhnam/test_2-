import pandas as pd
from sklearn.preprocessing import StandardScaler, LabelEncoder

def preprocess_data(df, is_train=True, scaler=None):
    # Lọc dữ liệu cơ bản
    df = filter_basic_conditions(df)

    # Xử lý các cột cụ thể
    df = process_gender_column(df)
    df = process_channel_column(df)
    df = process_special_columns(df)

    # Xử lý last_trading_date
    df = process_last_trading_date(df)

    # Tạo nhóm tuổi và lọc dữ liệu theo tuổi
    df = process_age_column(df)

    # Ghi lại master_account để dùng khi predict
    df_master = df.pop("master_account") if "master_account" in df.columns else None

    # Xử lý các cột kiểu object bằng LabelEncoder
    encoders = {}
    df, encoders = encode_object_columns(df, encoders)

    # Chuẩn hóa dữ liệu 
    if is_train:
        y = df.pop("is_inactive").values
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(df)
        return X_scaled, y, scaler, encoders
    else:
        if "is_inactive" in df.columns:
            df = df.drop(columns=["is_inactive"])
        X_scaled = scaler.transform(df)
        return X_scaled, df_master, encoders


# Các hàm xử lý riêng biệt
def filter_basic_conditions(df):
    """Lọc dữ liệu cơ bản."""
    df = df[df['open_time'] >= 0]
    df = df[df['customer_category'] == 1]
    return df


def process_gender_column(df):
    """Xử lý cột gender."""
    if "gender" in df.columns:
        df["gender"] = df["gender"].apply(lambda val: -999 if pd.isnull(val) or val == "unknown" else (0 if val == "F" else (1 if val == "M" else -999)))
    return df


def process_channel_column(df):
    """Xử lý cột channel."""
    if "channel" in df.columns:
        df["channel"] = (
            df["channel"]
            .replace({"unknown": -999, "nan": -999})
            .fillna(-999)
            .astype(str)
        )
    return df


def process_special_columns(df):
    """Xử lý các cột đặc biệt."""
    cols_special = ["age", "branch_code", "customer_category", "gender", "channel"]
    for c in cols_special:
        if c in df.columns:
            df[c] = df[c].apply(lambda x: -999 if pd.isnull(x) or x == "unknown" else x)

    # Các cột khác: null/unknown -> 0
    for c in df.columns:
        if c not in cols_special + ["is_inactive", "master_account", "last_trading_date"]:
            df[c] = df[c].apply(lambda x: 0 if pd.isnull(x) or x == "unknown" else x)
    return df


def process_last_trading_date(df):
    """Xử lý cột last_trading_date."""
    if "last_trading_date" in df.columns:
        df['last_trading_date'] = pd.to_datetime(df['last_trading_date'], errors='coerce')
        df['last_trading_year'] = df['last_trading_date'].dt.year.fillna(-999).astype(int)
        df['last_trading_month'] = df['last_trading_date'].dt.month.fillna(-999).astype(int)
        df['last_trading_day'] = df['last_trading_date'].dt.day.fillna(-999).astype(int)
        df.drop(columns=['last_trading_date'], inplace=True)
    return df


def process_age_column(df):
    """Tạo nhóm tuổi và lọc dữ liệu theo tuổi."""
    if "age" in df.columns:
        df['age_group'] = pd.cut(
            df['age'], bins=[15, 30, 50, 60, 80],
            labels=['Trẻ', 'Trung niên', 'Cao tuổi', 'Già'], right=False
        )
        df = df[(df['age'] > 15) & (df['age'] < 100)]
    return df


def encode_object_columns(df, encoders):
    """Mã hóa các cột kiểu object bằng LabelEncoder."""
    objects_columns = df.select_dtypes(include="object").columns
    for column in objects_columns:
        label_encoder = LabelEncoder()
        df[column] = label_encoder.fit_transform(df[column])
        encoders[column] = label_encoder
    return df, encoders