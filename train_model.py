import joblib
from sklearn.ensemble import GradientBoostingClassifier, StackingClassifier, AdaBoostClassifier
from xgboost import XGBClassifier
from lightgbm import LGBMClassifier
from sklearn.linear_model import LogisticRegression

def train_model_and_save(X, y, scaler, model_path="model.pkl", scaler_path="scaler.pkl"):
    # Các tham số tốt nhất từ GridSearchCV
    best_params = {
        'AdaBoost': {'learning_rate': 0.30000000000000004, 'n_estimators': 250},
        'LightGBM': {'learning_rate': 0.05, 'max_depth': 7, 'n_estimators': 150},
        'XGBoost': {'learning_rate': 0.05, 'max_depth': 7, 'n_estimators': 150},
        'GradientBoosting': {'learning_rate': 0.05, 'max_depth': 7, 'n_estimators': 150}
    }

    # Khởi tạo các mô hình với tham số tốt nhất
    models = [
        ('XGB', XGBClassifier(**best_params['XGBoost'], use_label_encoder=False, eval_metric="logloss")),
        ('LGBM', LGBMClassifier(**best_params['LightGBM'])),
        ('GB', GradientBoostingClassifier(**best_params['GradientBoosting'])),
        ('AdaBoost', AdaBoostClassifier(**best_params['AdaBoost']))
    ]

    # Meta-model với random_state và max_iter
    meta_model = LogisticRegression(random_state=42, max_iter=1000)

    # Stacking model
    stacking_model = StackingClassifier(estimators=models, final_estimator=meta_model)
    stacking_model.fit(X, y)

    # Lưu mô hình và scaler
    joblib.dump(stacking_model, model_path)
    joblib.dump(scaler, scaler_path)

    return stacking_model