"""
A final, complete experimentation engine to test feature impact on both
one-step accuracy (RMSE & R²) and long-term recursive stability (RMSE & R²).
"""
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.metrics import root_mean_squared_error, r2_score
import lightgbm as lgb
import database
from database import models
import warnings

warnings.filterwarnings("ignore", category=UserWarning)

# --- 1. Load and Prepare Data Once ---
session = database.sessions()
df = pd.read_sql_query(session.query(models.DataEntry).statement, session.bind)
df.sort_values(by=["country_code", "year"], inplace=True)
target_col = "logged_gdp_pcp"
df[target_col] = np.log(df["gdp"])
all_possible_lagged_features = [
    "population", "female", "male", "life_expectancy", "migration", "infant_mortality",
    "internet", "hci", "enrollment", "urban_pop", target_col,
]
for feature in all_possible_lagged_features:
    df[f"{feature}_lagged"] = df.groupby("country_code")[feature].shift(1)
df_clean = df.dropna().copy()

# --- 2. The Core Experiment Function (with Full Metrics) ---
def run_experiment(features, description):
    """Trains, evaluates (RMSE & R2), and runs a full recursive forecast."""
    print(f"--- Running Experiment: {description} ---")

    X = df_clean[features]
    y = df_clean[target_col]

    TRAINING_END = 2014
    train_mask = X["year"] <= TRAINING_END
    X_train, y_train = X[train_mask], y[train_mask]
    X_test, y_test = X[~train_mask], y[~train_mask]
    y_test_unlogged = np.exp(y_test)

    has_country_code = 'country_code' in features
    categorical_features = ['country_code'] if has_country_code else []
    numerical_features = [f for f in features if f not in categorical_features]

    mlr_pipeline = Pipeline(steps=[("preprocessor", ColumnTransformer(transformers=[("num", StandardScaler(), numerical_features), ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_features)])), ("regressor", LinearRegression())])
    lgbm_pipeline = Pipeline(steps=[("preprocessor", ColumnTransformer(transformers=[("cat", OneHotEncoder(handle_unknown="ignore"), categorical_features)], remainder="passthrough")), ("regressor", lgb.LGBMRegressor(random_state=42))])

    # One-Step Evaluation
    mlr_pipeline.fit(X_train, y_train)
    mlr_pred_log = mlr_pipeline.predict(X_test)
    mlr_rmse = root_mean_squared_error(y_test_unlogged, np.exp(mlr_pred_log))
    mlr_r2 = r2_score(y_test, mlr_pred_log)

    lgbm_pipeline.fit(X_train, y_train)
    lgbm_pred_log = lgbm_pipeline.predict(X_test)
    lgbm_rmse = root_mean_squared_error(y_test_unlogged, np.exp(lgbm_pred_log))
    lgbm_r2 = r2_score(y_test, lgbm_pred_log)

    # Recursive Forecasting
    mlr_rec_final, lgbm_rec_final = {}, {}
    lagged_target_col = f"{target_col}_lagged"

    if lagged_target_col in features:
        def recursive_forecast(model, X_test_data, y_test_data):
            forecast_df = X_test_data.copy()
            baseyear = forecast_df["year"].min()
            is_flexible_mode = 'country_code' in forecast_df.columns

            baseline_bootstrap = forecast_df[forecast_df["year"] == baseyear]
            if is_flexible_mode:
                baseline = pd.Series(baseline_bootstrap[lagged_target_col].values, index=baseline_bootstrap['country_code'])
            else:
                baseline = baseline_bootstrap[lagged_target_col].values

            scores = []
            for year in range(baseyear, forecast_df["year"].max() + 1):
                inp = forecast_df[forecast_df["year"] == year].copy()
                if is_flexible_mode:
                    inp[lagged_target_col] = inp["country_code"].map(baseline)
                else:
                    inp[lagged_target_col] = baseline

                prediction_log = model.predict(inp)
                true_log_values = y_test_data.loc[inp.index]
                rmse = root_mean_squared_error(np.exp(true_log_values), np.exp(prediction_log))
                r2 = r2_score(true_log_values, prediction_log)
                scores.append({'rmse': rmse, 'r2': r2})

                if is_flexible_mode:
                    baseline = pd.Series(prediction_log, index=inp['country_code'])
                else:
                    baseline = prediction_log
            return scores

        X_rec_test, y_rec_test = X_test, y_test
        if not has_country_code:
            test_meta_df = df_clean.loc[X_test.index][['country_code', 'year']]
            common_countries = set(test_meta_df[test_meta_df['year'] == test_meta_df['year'].min()]['country_code'])
            for year in range(test_meta_df['year'].min() + 1, test_meta_df['year'].max() + 1):
                common_countries.intersection_update(test_meta_df[test_meta_df['year'] == year]['country_code'])

            stable_indices = test_meta_df[test_meta_df['country_code'].isin(common_countries)].index
            X_rec_test = X_test.loc[stable_indices]
            y_rec_test = y_test.loc[stable_indices]

        if not X_rec_test.empty:
            mlr_rec_final = recursive_forecast(mlr_pipeline, X_rec_test, y_rec_test)[-1]
            lgbm_rec_final = recursive_forecast(lgbm_pipeline, X_rec_test, y_rec_test)[-1]

    return {
        "Scenario": description,
        "MLR_RMSE": mlr_rmse,
        "MLR_R2": mlr_r2,
        "LGBM_RMSE": lgbm_rmse,
        "LGBM_R2": lgbm_r2,
        "MLR_RMSE_Recursive_Final": mlr_rec_final.get('rmse', np.nan),
        "MLR_R2_Recursive_Final": mlr_rec_final.get('r2', np.nan),
        "LGBM_RMSE_Recursive_Final": lgbm_rec_final.get('rmse', np.nan),
        "LGBM_R2_Recursive_Final": lgbm_rec_final.get('r2', np.nan),
    }

# --- 3. Define and Run Scenarios ---
base_features = [f"{f}_lagged" for f in all_possible_lagged_features] + ["year"]

# --- Define Feature Sets for Clarity ---
# All possible socio-demographic features (the "Why" features)
socio_dem_features = [
    "population_lagged", "female_lagged", "male_lagged", "life_expectancy_lagged",
    "migration_lagged", "infant_mortality_lagged", "internet_lagged", "hci_lagged",
    "enrollment_lagged", "urban_pop_lagged"
]

# The single most powerful forecasting feature
momentum_feature = [f"{target_col}_lagged"]

# --- Define Scenarios to Test ---
scenarios_to_test = [
    # --- Group 1: The "Explanatory" or "Why" Models (NO Lagged GDP) ---
    # These models try to explain wealth from first principles.
    {
        "description": "Explanatory - Contextual (All Socio-Dem + Countries)",
        "features": ["year", "country_code"] + socio_dem_features,
    },
    {
        "description": "Explanatory - Universal (All Socio-Dem, No Countries)",
        "features": ["year"] + socio_dem_features,
    },
    {
        "description": "Explanatory - Human Development Focus (With Countries)",
        "features": ["year", "country_code", "life_expectancy_lagged", "infant_mortality_lagged", "hci_lagged", "enrollment_lagged"],
    },
    {
        "description": "Explanatory - Tech & Health Focus (With Countries)",
        "features": ["year", "country_code", "internet_lagged", "life_expectancy_lagged", "infant_mortality_lagged"],
    },

    # --- Group 2: The "Forecasting" or "Momentum" Models (WITH Lagged GDP) ---
    # These models are designed for pure prediction, relying heavily on the previous year's GDP.
    {
        "description": "Forecast - Pure Momentum (GDP Lag)",
        "features": ["year"] + momentum_feature,
    },
    {
        "description": "Forecast - Localized Momentum (GDP Lag + Countries)",
        "features": ["year", "country_code"] + momentum_feature,
    },
    {
        "description": "Forecast - Full Model (All Socio-Dem + GDP Lag + Countries)",
        "features": ["year", "country_code"] + socio_dem_features + momentum_feature,
    },

    # --- Group 3: Baselines ---
    {
        "description": "Baseline - No Features (Year Only)",
        "features": ["year"],
    },
]

results = []
for scenario in scenarios_to_test:
    results.append(run_experiment(features=scenario['features'], description=scenario['description']))

# --- 4. Display Final Summary Table ---
results_df = pd.DataFrame(results).set_index("Scenario")
print("\n\n--- EXPERIMENT SUMMARY ---")
print(results_df.to_string(formatters={
    'MLR_RMSE': '{:,.2f}'.format,
    'LGBM_RMSE': '{:,.2f}'.format,
    'MLR_R2': '{:.4f}'.format,
    'LGBM_R2': '{:.4f}'.format,
    'MLR_RMSE_Rec_Final': '{:,.2f}'.format,
    'LGBM_RMSE_Rec_Final': '{:,.2f}'.format,
    'MLR_R2_Rec_Final': '{:.4f}'.format,
    'LGBM_R2_Rec_Final': '{:.4f}'.format,
}))

import matplotlib.pyplot as plt
import numpy as np

# --- 5. Visualize the Experiment Results ---
print("\n--- Generating Result Visualizations ---")

def add_bar_labels(ax, rects1, rects2, is_r2=False):
    """Attach a text label above each bar in *rects*, displaying its height."""
    fmt = '{:.4f}' if is_r2 else '{:,.0f}'
    ax.bar_label(rects1, padding=3, fmt=fmt, rotation=90, fontsize=9)
    ax.bar_label(rects2, padding=3, fmt=fmt, rotation=90, fontsize=9)

# --- Plot 1: One-Step-Ahead RMSE Comparison ---
plt.style.use('seaborn-v0_8-whitegrid')
fig, ax = plt.subplots(figsize=(12, 9)) # Increased figure height for labels

scenarios = results_df.index
x = np.arange(len(scenarios))
width = 0.35

rects1 = ax.bar(x - width/2, results_df['MLR_RMSE'], width, label='MLR RMSE', color='skyblue')
rects2 = ax.bar(x + width/2, results_df['LGBM_RMSE'], width, label='LGBM RMSE', color='royalblue')

ax.set_ylabel('RMSE (in dollars of GDP per capita)')
ax.set_title('One-Step-Ahead Forecast Accuracy Comparison', fontsize=16)
ax.set_xticks(x)
ax.set_xticklabels(scenarios, rotation=45, ha="right")
ax.legend()
ax.set_yscale('log')
ax.grid(True, which="both", ls="--", c='0.7')
add_bar_labels(ax, rects1, rects2)

fig.tight_layout()
plt.savefig("one_step_rmse_comparison.png")
print("Saved one-step RMSE comparison plot to 'one_step_rmse_comparison.png'")


# --- Plot 2: Recursive Forecast Stability Comparison ---
recursive_df = results_df.dropna(subset=['MLR_RMSE_Recursive_Final', 'LGBM_RMSE_Recursive_Final'])
if not recursive_df.empty:
    fig2, ax2 = plt.subplots(figsize=(12, 9))
    rec_scenarios = recursive_df.index
    x_rec = np.arange(len(rec_scenarios))

    rects3 = ax2.bar(x_rec - width/2, recursive_df['MLR_RMSE_Recursive_Final'], width, label='MLR Final Recursive RMSE', color='lightcoral')
    rects4 = ax2.bar(x_rec + width/2, recursive_df['LGBM_RMSE_Recursive_Final'], width, label='LGBM Final Recursive RMSE', color='firebrick')

    ax2.set_ylabel('RMSE of Final Year Forecast (Error Accumulation)')
    ax2.set_title('Long-Term Forecast Stability Comparison (Recursive Test)', fontsize=16)
    ax2.set_xticks(x_rec)
    ax2.set_xticklabels(rec_scenarios, rotation=45, ha="right")
    ax2.legend()
    ax2.grid(True, ls="--", c='0.7')
    add_bar_labels(ax2, rects3, rects4)

    fig2.tight_layout()
    plt.savefig("recursive_stability_comparison.png")
    print("Saved recursive stability comparison plot to 'recursive_stability_comparison.png'")

# --- Plot 3: One-Step-Ahead R-squared Comparison ---
fig3, ax3 = plt.subplots(figsize=(12, 9))
rects5 = ax3.bar(x - width/2, results_df['MLR_R2'], width, label='MLR R²', color='mediumseagreen')
rects6 = ax3.bar(x + width/2, results_df['LGBM_R2'], width, label='LGBM R²', color='darkgreen')

ax3.set_ylabel('R-squared Score')
ax3.set_title('One-Step-Ahead R-squared Comparison', fontsize=16)
ax3.set_xticks(x)
ax3.set_xticklabels(scenarios, rotation=45, ha="right")
ax3.legend()
ax3.set_ylim([-0.1, 1.05]) # Give a little extra space at the top for labels
ax3.grid(True, ls="--", c='0.7')
add_bar_labels(ax3, rects5, rects6, is_r2=True)

fig3.tight_layout()
plt.savefig("one_step_r2_comparison.png")
print("Saved one-step R-squared comparison plot to 'one_step_r2_comparison.png'")

# --- Plot 4: Recursive Forecast R-squared Comparison ---
if not recursive_df.empty:
    fig4, ax4 = plt.subplots(figsize=(12, 9))
    rects7 = ax4.bar(x_rec - width/2, recursive_df['MLR_R2_Recursive_Final'], width, label='MLR Final Recursive R²', color='orchid')
    rects8 = ax4.bar(x_rec + width/2, recursive_df['LGBM_R2_Recursive_Final'], width, label='LGBM Final Recursive R²', color='darkviolet')

    ax4.set_ylabel('R-squared Score of Final Year Forecast')
    ax4.set_title('Long-Term Forecast R-squared Comparison (Recursive Test)', fontsize=16)
    ax4.set_xticks(x_rec)
    ax4.set_xticklabels(rec_scenarios, rotation=45, ha="right")
    ax4.legend()
    ax4.set_ylim([0, 1.05]) # Give a little extra space at the top
    ax4.grid(True, ls="--", c='0.7')
    add_bar_labels(ax4, rects7, rects8, is_r2=True)

    fig4.tight_layout()
    plt.savefig("recursive_r2_comparison.png")
    print("Saved recursive R-squared comparison plot to 'recursive_r2_comparison.png'")

print("\nAll plotting is done!")

print("\nAll done!")