import joblib
import pandas as pd

def predict_with_model(model, X_input, master_accounts):
    y_pred = model.predict(X_input)
    y_prob = model.predict_proba(X_input)[:, 1]

    df_result = pd.DataFrame({
        "master_account": master_accounts.values,
        "prediction": y_pred,
        "probability": y_prob
    })

    return df_result
