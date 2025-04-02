from query_data import query_train_data, query_predict_data
from preprocess import preprocess_data
from train_model import train_model_and_save
from predict_model import predict_with_model


def main():
    try:
        # 1. Query dữ liệu
        print("🔄 Đang query dữ liệu huấn luyện...")
        df_train_raw = query_train_data()
        if df_train_raw is None or df_train_raw.empty:
            raise ValueError("Dữ liệu huấn luyện bị rỗng hoặc không hợp lệ.")
        print("✅ Query dữ liệu huấn luyện hoàn tất!")

        print("🔄 Đang query dữ liệu dự đoán...")
        df_pred_raw = query_predict_data()
        if df_pred_raw is None or df_pred_raw.empty:
            raise ValueError("Dữ liệu dự đoán bị rỗng hoặc không hợp lệ.")
        print("✅ Query dữ liệu dự đoán hoàn tất!")

        # 2. Tiền xử lý
        print("🔄 Đang tiền xử lý dữ liệu huấn luyện...")
        X_train, y_train, scaler, label_encoders = preprocess_data(df_train_raw, is_train=True)
        print("✅ Tiền xử lý dữ liệu huấn luyện hoàn tất!")

        print("🔄 Đang huấn luyện mô hình...")
        model = train_model_and_save(X_train, y_train, scaler)
        print("✅ Huấn luyện mô hình hoàn tất!")

        print("🔄 Đang tiền xử lý dữ liệu dự đoán...")
        X_pred, master_accounts = preprocess_data(df_pred_raw, is_train=False, scaler=scaler, encoders=label_encoders)
        print("✅ Tiền xử lý dữ liệu dự đoán hoàn tất!")

        # 3. Dự đoán
        print("🔄 Đang dự đoán...")
        df_result = predict_with_model(model, X_pred, master_accounts)
        print("✅ Dự đoán hoàn tất!")

        # 4. In kết quả
        print("🔄 Kết quả dự đoán:")
        print(df_result.head())
        print("✅ Pipeline hoàn tất!")

    except Exception as e:
        print(f"❌ Lỗi xảy ra trong pipeline: {e}")


if __name__ == "__main__":
    main()