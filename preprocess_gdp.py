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
    combined_df = pd.merge(clean_data, df, on=["alpha_2", "year"], how="left")
    desired_order = [
        "name_x", "name_y", "name", "alpha_2", "iso_2",  "abb", "year", "population", "pop", "GDP",
        "christian", "islam", "buddhist", "judaism", "nonrelig", "other"
    ]
    combined_df = combined_df[[col for col in desired_order if col in combined_df.columns]]
    combined_df.reset_index(drop=True).sort_values(["alpha_2", "year"])
    names = combined_df["name_x"].unique()
    countries = Data.get_countries_with_min_range(combined_df, 1960, 2010)
    drop_names = [n for n in names if n not in countries]
    cleaned_df = combined_df[~combined_df["name_x"].isin(drop_names)].dropna().reset_index(drop=True)
    # print(list(cleaned_df))
    # cleaned_df = cleaned_df.drop(columns=['Unnamed: 0'])
    cleaned_df.to_csv("data/cleaned_pop_gdp_relig_df.csv")
else:
    cleaned_df = pd.read_csv("data/cleaned_pop_gdp_relig_df.csv")

cleaned_df.head(5)
