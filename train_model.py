import joblib
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier, StackingClassifier
from xgboost import XGBClassifier
from lightgbm import LGBMClassifier
from catboost import CatBoostClassifier
from sklearn.linear_model import LogisticRegression

def train_model_and_save(X, y, scaler, model_path="model.pkl", scaler_path="scaler.pkl"):
    models = [
        ('RF', RandomForestClassifier(n_estimators=100, random_state=42)),
        ('XGB', XGBClassifier(use_label_encoder=False, eval_metric="logloss")),
        ('LGBM', LGBMClassifier()),
        ('GB', GradientBoostingClassifier(n_estimators=100, random_state=42)),
        ('CatBoost', CatBoostClassifier(verbose=0))
    ]
    meta_model = LogisticRegression()
    stacking_model = StackingClassifier(estimators=models, final_estimator=meta_model)
    stacking_model.fit(X, y)

    joblib.dump(stacking_model, model_path)
    joblib.dump(scaler, scaler_path)

    return stacking_model
