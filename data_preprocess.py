import os
import requests
import pandas as pd
import numpy as np
from scipy.interpolate import PchipInterpolator

def solve_missing_values(y, x):
    all_years = np.arange(min(x), max(x) + 1, 1)
    interp = PchipInterpolator(x, y)
    return interp(all_years), all_years

class Data:
    POP_DATA_URL = "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Demographic_Indicators_Medium.csv.gz"
    POP_PROCESSED_PATH = "data/population_country_data.csv"

    RELIG_DATA_URL = "https://correlatesofwar.org/wp-content/uploads/WRP_national.csv"
    RELIG_PROCESSED_PATH = "data/religion_country_data.csv"  

    @staticmethod
    def download_file(url, folder="data"):
        os.makedirs(folder, exist_ok=True)
        filename = url.split("/")[-1]
        local_path = os.path.join(folder, filename)
        if os.path.exists(local_path):
            print(f"Using cached file: {local_path}")
            return local_path
        print(f"Downloading: {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Saved to: {local_path}")
        return local_path
    
    @staticmethod
    def open_gz_with_pandas(file_path):
        """Open a gzip-compressed CSV file with pandas."""
        return pd.read_csv(file_path, compression='gzip', low_memory=False)
    
    @staticmethod
    def get_pop_raw_df():
        file_path = Data.download_file(Data.POP_DATA_URL)
        return Data.open_gz_with_pandas(file_path)
    
    def get_pop_df():
        df = Data.get_pop_raw_df()
        df = df[df['LocTypeID'] == 4][['Location', 'Time', 'ISO2_code', 'TPopulation1Jan']]
        df.loc[df["Location"] == "Namibia", "ISO2_code"] = "NA"
        df.drop(["Location"], axis=1, inplace=True)
        df = df[df["Time"] > 1969]
        df = df[df["Time"] < 2025]
        df["TPopulation1Jan"] = (df["TPopulation1Jan"]*1_000).astype(int)
        df.rename(columns={"TPopulation1Jan": "Population"}, inplace=True)
        return df.reset_index(drop=True)

    @staticmethod
    def get_relig_raw_df():
        file_path = Data.download_file(Data.RELIG_DATA_URL)
        return pd.read_csv(file_path)

    @staticmethod
    def fix_vietnam(df):
        # Split subsets
        north = df[df["name"] == "DRV"]
        south = df[df["name"] == "RVN"]

        # --- Step 1: Combine North + South ---
        combined = (
            pd.concat([north, south])
            .groupby("year", as_index=False)
            .sum(numeric_only=True)
        )
        combined["name"] = "VN"

        df_clean = df[~df["name"].isin(["DRV", "RVN", "VN", "VNM", "Vietnam"])]

        # --- Step 5: Append new Vietnam ---
        df_final = pd.concat([df_clean, combined], ignore_index=True)
        df_final = df_final.sort_values(["name", "year"]).reset_index(drop=True)

        return df_final


    @staticmethod
    def fix_germany(df):
        # Split out subsets
        east  = df[df["name"] == "GDR"]
        west  = df[df["name"] == "GFR"]
        gmy   = df[df["name"] == "GMY"]

        # --- Step 1: East + West (before 1990) ---
        east_west_sum = (
            pd.concat([east, west])
            .groupby("year", as_index=False)
            .sum(numeric_only=True)
        )
        east_west_sum["name"] = "DE"

        # --- Step 2: Unified Germany from 1990 ---
        unified = gmy.copy()
        unified["name"] = "DE"
        unified = unified[unified["year"] >= 1990]

        # --- Step 3: Fix 1990 by adding East Germany into GMY ---
        east_1990 = east[east["year"] == 1990]
        if not east_1990.empty:
            # add east counts to unified 1990
            unified.loc[unified["year"] == 1990, df.select_dtypes(include="number").columns] += \
                east_1990[df.select_dtypes(include="number").columns].values

        # --- Step 4: Combine ---
        germany_df = pd.concat([east_west_sum[east_west_sum["year"] < 1990], unified], ignore_index=True)

        # --- Step 5: Remove old GDR/GFR/GMY ---
        df_clean = df[~df["name"].isin(["GDR", "GFR", "GMY"])].copy()

        # --- Step 6: Append Germany back ---
        df_final = pd.concat([df_clean, germany_df], ignore_index=True)
        df_final = df_final.sort_values(["name", "year"]).reset_index(drop=True)
        return df_final

    @staticmethod
    def get_relig_df():
        df = Data.get_relig_raw_df()
        codes = pd.read_csv(Data.download_file("https://correlatesofwar.org/wp-content/uploads/COW-country-codes.csv"))
        code_map = codes.drop_duplicates(subset="CCode").set_index("CCode")["StateNme"]
        df["name"] = df["state"].map(code_map)
        relig_df = pd.DataFrame({
            "name": df["name"],
            "year": df["year"],
            "christian": df["chrstgen"],
            
            "islam": df["islmgen"],
            
            "buddhist": df["budgen"],
            
            "nonrelig": df["nonrelig"],

            "judaism": df["judgen"],
            
            "other": ( df["zorogen"] + df["hindgen"] + df["sikhgen"] + df["shntgen"] +
                        df["bahgen"] + df["taogen"] + df["jaingen"] + df["confgen"] +
                    df["syncgen"] + df["anmgen"] + df["othrgen"]
            ),
            "pop": df["pop"]
        })
        relig_df = Data.fix_germany(relig_df)
        relig_df = Data.fix_vietnam(relig_df)
        return relig_df
    
    @staticmethod
    def enrich_relig_df(relig_df):
        countries = relig_df["name"].unique()
        solved_dfs = []
        columns = ["christian", "islam", "buddhist", "judaism", "nonrelig", "other", "pop"]
        for country in countries:
            if country in ["Kosovo", "Montenegro"]:
                continue
            country_rows = relig_df.loc[(relig_df["name"] == country)]
            x = country_rows["year"].unique()
            all_years = np.arange(min(x), max(x) + 1, 1)
            df = pd.DataFrame({
                "year": all_years,
                "name": [country for _ in range(len(all_years))]
            })
            for column in columns:
                try:
                    y, _ = solve_missing_values(country_rows[column], x)
                    df[column] = y
                    df[column] = df[column].astype(int) * 1000
                except:
                    print(country, country_rows[column], column)
                    exit()
            solved_dfs.append(df)
        new_df = pd.concat(solved_dfs, ignore_index=True)
        return new_df

if __name__ == "__main__":
    relig_raw_df = Data.get_relig_df()
    relig_df = Data.enrich_relig_df(relig_raw_df)
    relig_df.to_csv("data/new_relig_df.csv")

    # relig_raw_df.to_csv(Data.RELIG_PROCESSED_PATH)

    # pop_df = Data.get_pop_df()
    # pop_df.to_csv(Data.POP_PROCESSED_PATH)
    # print(pop_df.head(5))
