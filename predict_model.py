import joblib
import pandas as pd

def predict_with_model(model, X_input, master_accounts, threshold=0.45):
 
    # Kiểm tra master_accounts
    if master_accounts is None:
        raise ValueError("master_accounts cannot be None. Ensure 'master_account' column exists in the input data.")
    
    # Đảm bảo master_accounts là pd.Series
    if not isinstance(master_accounts, (pd.Series, pd.DataFrame)):
        master_accounts = pd.Series(master_accounts, name="master_account")
    
    # Kiểm tra mô hình hỗ trợ predict_proba
    if not hasattr(model, "predict_proba"):
        raise AttributeError("The provided model does not support predict_proba.")
    
    # Dự đoán xác suất
    y_prob = model.predict_proba(X_input)[:, 1]

    # Điều chỉnh dự đoán với ngưỡng
    y_pred_adjusted = (y_prob >= threshold).astype(int)

    # Tạo DataFrame kết quả
    df_result = pd.DataFrame({
        "master_account": master_accounts.values,
        "prediction": y_pred_adjusted,
        "probability": y_prob
    })

    return df_result