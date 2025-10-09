"""Actual modeling of the data!"""
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import root_mean_squared_error, r2_score
import lightgbm as lgb

import database
from database import models
import pandas as pd
import numpy as np

session = database.sessions()
df = pd.read_sql_query(session.query(models.DataEntry).statement, session.bind)

df.sort_values(by=["country_code", "year"], inplace=True)

target = "logged_gdp_pcp"
df[target] = np.log(df["gdp"])

lagged_features = [
    "population",
    "female",
    "male",
    "life_expectancy",
    "migration",
    "infant_mortality",
    "internet",
    "hci",
    "enrollment",
    "urban_pop",
    target,
]
features = ["country_code", "year"]

for feature in lagged_features:
    df[f"{feature}_lagged"] = df.groupby("country_code")[feature].shift(1)
    features.append(f"{feature}_lagged")

df.dropna(inplace=True)
X = df[features]
y = df[target]

TRAINING_END = 2020
train_mask = X["year"] <= TRAINING_END

X_train = X[train_mask]
y_train = y[train_mask]
X_test = X[~train_mask]
y_test = y[~train_mask]
y_test_unlogged = np.exp(y_test)

categorical_features = ["country_code"]
numerical_features = [feature for feature in features if feature not in categorical_features]

mlr_preprocessor = ColumnTransformer(
    transformers=[
        ("num", StandardScaler(), numerical_features),
        ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_features),
    ],
)

mlr_mainline = Pipeline(steps=[
    ("preprocessor", mlr_preprocessor),
    ("regressor", LinearRegression()),
])

lgbm_preprocessor = ColumnTransformer(
    transformers=[
        ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_features),
    ],
    remainder="passthrough",
)

lgbm_mainline = Pipeline(steps=[
    ("preprocessor", lgbm_preprocessor),
    ("regressor", lgb.LGBMRegressor(random_state=42, force_col_wise=True)),
])

print("Training MLR!")
mlr_mainline.fit(X_train, y_train)
print("Done! Testing MLR")
mlr_predictions = mlr_mainline.predict(X_test)
restored_mlr_predictions = np.exp(mlr_predictions)
mlr_rmse = root_mean_squared_error(y_test_unlogged, restored_mlr_predictions)
mlr_r2 = r2_score(y_test, mlr_predictions)
print("MLR RMSE: ", mlr_rmse)
print("MLR R2: ", mlr_r2)
print("Done! Traning LGBM!")
lgbm_mainline.fit(X_train, y_train)
print("Done! Testing LGBM")
lgbm_predictions = lgbm_mainline.predict(X_test)
restored_lgbm_predictions = np.exp(lgbm_predictions)
lgbm_rmse = root_mean_squared_error(y_test_unlogged, restored_lgbm_predictions)
lgbm_r2 = r2_score(y_test, lgbm_predictions)
print("LGBM RMSE: ", lgbm_rmse)
print("LGBM R2: ", lgbm_r2)