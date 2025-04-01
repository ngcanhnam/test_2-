from query_data import query_train_data, query_predict_data
from preprocess import preprocess_data
from train_model import train_model_and_save
from predict_model import predict_with_model


def main():
    # 1. Query dữ liệu
    df_train_raw = query_train_data()
    df_pred_raw = query_predict_data()


    # 2. Tiền xử lý
    X_train, y_train, scaler = preprocess_data(df_train_raw, is_train=True)
    model = train_model_and_save(X_train, y_train, scaler)

    # 3. Dự đoán
    X_pred, master_accounts = preprocess_data(df_pred_raw, is_train=False, scaler=scaler)
    df_result = predict_with_model(model, X_pred, master_accounts)

    # 4. In kết quả
    print(df_result.head())
    print("✅ Pipeline hoàn tất!")


if __name__ == "__main__":
    main()
