import pandas as pd
from sklearn.preprocessing import StandardScaler

def preprocess_data(df, is_train=True, scaler=None):
    df = df[df['open_time'] >= 0]
    df = df[df['customer_category'] != 0]

    # Xử lý gender
    def convert_gender(val):
        if pd.isnull(val) or val == "unknown": return -999
        if val == "F": return 0
        elif val == "M": return 1
        try:
            val = float(val)
            return val if val in [0.0, 1.0] else -999
        except: return -999
    df["gender"] = df["gender"].apply(convert_gender)

    # Xử lý channel
    def convert_channel(val):
        if pd.isnull(val) or val == "unknown": return -999
        elif val == "MBS": return 0
        elif val == "Organic MBB": return 1
        elif val == "CJ MBB": return 2
        else: return -999
    if "channel" in df.columns:
        df["channel"] = df["channel"].apply(convert_channel)

    # Các cột đặc biệt: điền -999 nếu null/unknown
    cols_special = ["age", "branch_code", "customer_category", "gender", "channel"]
    for c in cols_special:
        if c in df.columns:
            df[c] = df[c].apply(lambda x: -999 if pd.isnull(x) or x == "unknown" else x)

    # Các cột khác: null/unknown → 0
    for c in df.columns:
        if c not in cols_special + ["is_inactive", "master_account", "last_trading_date"]:
            df[c] = df[c].apply(lambda x: 0 if pd.isnull(x) or x == "unknown" else x)

    # last_trading_date: tách thành year/month/day
    if "last_trading_date" in df.columns:
        df['last_trading_date'] = pd.to_datetime(df['last_trading_date'], errors='coerce')
        df['last_trading_year'] = df['last_trading_date'].dt.year.fillna(-999).astype(int)
        df['last_trading_month'] = df['last_trading_date'].dt.month.fillna(-999).astype(int)
        df['last_trading_day'] = df['last_trading_date'].dt.day.fillna(-999).astype(int)
        df.drop(columns=['last_trading_date'], inplace=True)

    # Ghi lại master_account để dùng khi predict
    if "master_account" in df.columns:
        df_master = df["master_account"]
        df = df.drop(columns=["master_account"])
    else:
        df_master = None

    # Với train: lấy y
    if is_train:
        y = df["is_inactive"].values
        df = df.drop(columns=["is_inactive"])
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(df)
        return X_scaled, y, scaler
    else:
        if "is_inactive" in df.columns:
            df = df.drop(columns=["is_inactive"])
        X_scaled = scaler.transform(df)
        return X_scaled, df_master
