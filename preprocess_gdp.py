import pandas as pd
from data_preprocess import Data

redo = True

if redo:
    df = pd.read_csv("data_gdp/gdp_data.csv")
    column_rename_map = {
        "ISO2_code": "ISO2_code",
        "Country Name": "name",
        "Time": "year",
    }
    df.rename(columns=column_rename_map, inplace=True)
    df = Data.add_a2_values(df)
    df.to_csv("data/clean_gdp_df.csv")
else:
    df = pd.read_csv("data/clean_gdp_df.csv").dropna()

clean_data = pd.read_csv("data\cleaned_pop_relig_df.csv")

if redo:
    combined_df = pd.merge(clean_data, df, on=["alpha_2", "year"], how="outer")
    desired_order = [
        "name", "alpha_2", "iso_2",  "abb", "year", "population", "pop", "GDP",
        "christian", "islam", "buddhist", "judaism", "nonrelig", "other"
    ]
    combined_df["name"] = combined_df.apply(
            lambda row: row["name_y"] if pd.notna(row["name_y"]) else row["name_x"],
            axis=1
        )
    combined_df.drop(columns=["name_x", "name_y"], inplace=True)
    combined_df = combined_df[[col for col in desired_order if col in combined_df.columns]]
    combined_df.reset_index(drop=True).sort_values(["alpha_2", "year"])
    if False:
        all_names = combined_df["name_x"].unique()
        countries = Data.get_countries_with_min_range(combined_df, 1960, 2010)
        cleaned_df = combined_df[combined_df["name_x"].isin(countries)].dropna().reset_index(drop=True)
    else:
        cleaned_df = combined_df
    cleaned_df.to_csv("data/cleaned_pop_gdp_relig_df.csv")
else:
    cleaned_df = pd.read_csv("data/cleaned_pop_gdp_relig_df.csv")

# print(clean_data["name_x"].unique())
# for c in countries:
#     if c not in range_countries:
#         print(c)
