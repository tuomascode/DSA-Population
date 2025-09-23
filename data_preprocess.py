import os
import requests
import pandas as pd
import numpy as np
from tqdm import tqdm

from scipy.interpolate import PchipInterpolator
from database.utils import get_country

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
        print("Getting raw population data")
        file_path = Data.download_file(Data.POP_DATA_URL)
        return Data.open_gz_with_pandas(file_path)
    
    @staticmethod
    def rename_countries(df):

        remap = [
            ['China, Hong Kong SAR', 'Hong Kong'],
            ['China, Macao SAR', 'Macao'],
            ['China, Taiwan Province of China', 'Taiwan'],
            ['Iran (Islamic Republic of)', 'Iran'],
            ['United States Virgin Islands', 'Virgin Islands'],
            ['Bolivia (Plurinational State of)', 'Bolivia'],
            ['Venezuela (Bolivarian Republic of)', 'Venezuela'],
            ['Micronesia (Fed. States of)', 'Micronesia'],
            ['Wallis and Futuna Islands', 'Wallis'],
            ["Dem. People's Republic of Korea", "North Korea"],
            ["Republic of Korea", "South Korea"],
            ['Kosovo (under UNSC res. 1244)', "Kosovo"]
            ]

        df.loc[df["Location"] == "Namibia", "ISO2_code"] = "NA"
        df.loc[df["Location"] == "Niger", "ISO2_code"] = "NG"
        df.loc[df["Location"] == "Dem. People's Republic of Korea", "Location"] = "North Korea"
        df.loc[df["Location"] == "Republic of Korea", "Location"] = "South Korea"
        for cur, new in tqdm(remap, desc = "renaming"):
            df.loc[df["Location"] == cur, "Location"] = new

    def get_pop_df():
        df = Data.get_pop_raw_df()
        print("Parsing columns")
        df = df[df['LocTypeID'] == 4][['Location', 'Time', 'ISO2_code', 'TPopulation1Jan']]
        Data.rename_countries(df)
        # df.drop(["Location"], axis=1, inplace=True)
        df = df[df["Time"] > 1969]
        df = df[df["Time"] < 2025]
        df["TPopulation1Jan"] = (df["TPopulation1Jan"]*1_000).astype(int)
        print("renaming columns")
        column_rename_map = {
            "TPopulation1Jan": "population",
            "Location": "name",
            "Time": "year",
        }
        df.rename(columns=column_rename_map, inplace=True)

        df = Data.add_a2_values(df)
        df = Data.remove_split_countries(df, "alpha_2")
        return df.reset_index(drop=True).sort_values(["alpha_2", "year"]).reset_index(drop=True)

    @staticmethod
    def get_relig_raw_df():
        file_path = Data.download_file(Data.RELIG_DATA_URL)
        return pd.read_csv(file_path)

    @staticmethod
    def fix_vietnam(df):
        """
        Vietnam data is split due to historical reasons.
        Concidering vietnam as one entity.
        """
        print("Fixing Vietnam data")
        north = df[df["name"] == "DRV"]
        south = df[df["name"] == "RVN"]
        combined = (
            pd.concat([north, south])
            .groupby("year", as_index=False)
            .sum(numeric_only=True)
        )
        combined["name"] = "DRV"
        combined["abb"] = "DRV"
        df_clean = df[~df["name"].isin(["DRV", "RVN", "VN", "VNM", "Vietnam"])]
        df_final = pd.concat([df_clean, combined], ignore_index=True)
        df_final = df_final.sort_values(["name", "year"]).reset_index(drop=True)
        return df_final

    @staticmethod
    def fix_germany(df):
        """
        Germany data is split into three parts. Historical East and west germany.
        This function combines them all into one entity.
        """
        print("Fixing germany data")
        east  = df[df["name"] == "GDR"]
        west  = df[df["name"] == "GFR"]
        gmy   = df[df["name"] == "GMY"]
        east_west_sum = (
            pd.concat([east, west])
            .groupby("year", as_index=False)
            .sum(numeric_only=True)
        )
        east_west_sum["name"] = "GMY"
        unified = gmy.copy()
        unified["name"] = "GMY"
        unified = unified[unified["year"] >= 1990]
        east_1990 = east[east["year"] == 1990]
        if not east_1990.empty:
            unified.loc[unified["year"] == 1990, df.select_dtypes(include="number").columns] += \
                east_1990[df.select_dtypes(include="number").columns].values
        germany_df = pd.concat([east_west_sum[east_west_sum["year"] < 1990], unified], ignore_index=True)
        germany_df["abb"] = "GMY"
        df_clean = df[~df["name"].isin(["GDR", "GFR", "GMY"])].copy()
        df_final = pd.concat([df_clean, germany_df], ignore_index=True)
        df_final = df_final.sort_values(["name", "year"]).reset_index(drop=True)
        return df_final
    
    @staticmethod
    def remove_split_countries(df, name = "name"):
        """
        Some countries present major challenges due to a break. Especially Yugoslavia is difficult.
        Removing all of them is the simplest option.
        """
        print("Removing problematic countries data")
        removals = [
            "Yugoslavia",
            "YUG",
            "CRO", "HRV", "HR", "Croatia",
            "SLO", "SVN", "Slovenia",
            "SRB", "RS", "Serbia",
            "MNE", "ME", "MNG", "Montenegro",
            "BIH", "BA", "BOS", "Bosnia and Herzegovina",
            "MKD", "MK", "Macedonia", "MAC", "North Macedonia",
            "XK", "KOS", "Kosovo",
            "SLO", "CZE",
            "YAR", "YPR", "YEM", "Yemen",
            "PRK", # Korea
            ]

        return df[~df[name].isin(removals)]
    
    @staticmethod
    def get_a2_map(df):
        mapping = {}
        failed = []
        for c in tqdm(df["name"].unique(), "Solving alpha2 values"):
            try:
                mapping[c] = get_country(c).alpha_2
            except:
                failed.append(c)
        if failed:
            print(failed)
            raise KeyError(f"{', '.join(failed)} not found by get_country")
        return mapping

    @staticmethod
    def add_a2_values(df):
        print("Resolving country names to alpha_2")
        mapping = Data.get_a2_map(df)
        df.insert(loc = 2, column = "alpha_2", value = df["name"].map(mapping)) 
        return df
    
    @staticmethod
    def map_name_to_a2(df, column):
        mapping = Data.get_a2_map()
        df[column] = df[column].map(mapping)

    @staticmethod
    def get_relig_df():
        df = Data.get_relig_raw_df()
        relig_df = pd.DataFrame({
            "name": df["name"],
            "abb": df["name"],
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
        relig_df = Data.remove_split_countries(relig_df)
        codes = pd.read_csv(Data.download_file("https://correlatesofwar.org/wp-content/uploads/COW-country-codes.csv")).drop_duplicates("StateAbb")
        print("Resolving country names to COW statename")
        code_map = codes.set_index("StateAbb")["StateNme"]
        relig_df["name"] = relig_df["name"].map(code_map)
        relig_df = Data.add_a2_values(relig_df)
        print("Base relig data processed")
        return relig_df.sort_values(["alpha_2", "year"]).reset_index(drop=True)
    
    @staticmethod
    def enrich_relig_df(relig_df):
        print("Solving missing values for relig df")
        countries = relig_df["name"].unique()
        solved_dfs = []
        columns = ["christian", "islam", "buddhist", "judaism", "nonrelig", "other", "pop"]
        for country in tqdm(countries, desc="Processing missing values"):
            country_rows = relig_df.loc[(relig_df["name"] == country)]
            x = country_rows["year"].unique()
            all_years = np.arange(min(x), max(x) + 1, 1)
            df = pd.DataFrame({
                "year": all_years,
                "name": [country for _ in range(len(all_years))],
                "alpha_2" : country_rows["alpha_2"].unique()[0],
                "abb" : country_rows["abb"].unique()[0],
            })
            for column in columns:
                try:
                    y, _ = solve_missing_values(country_rows[column], x)
                    df[column] = y
                    df[column] = df[column].astype(int)
                except:
                    print(country, country_rows[column], column)
                    exit()
            solved_dfs.append(df)
        new_df = pd.concat(solved_dfs, ignore_index=True)
        columns = ["christian", "islam", "buddhist", "judaism", "nonrelig", "other"]
        for col in tqdm(columns, desc="Changing values to relative values"):
            new_df[col] = new_df[col] / new_df["pop"]
        
        return new_df.sort_values(["alpha_2", "year"]).reset_index(drop=True)
    
    @staticmethod
    def join_tables(pop_df, relig_df):
        print("combining tables")
        # relig_df.drop(["abb", "name"], axis=1, inplace=True)
        combined_df = pd.merge(pop_df, relig_df, on=["alpha_2", "year"], how="inner")
        return combined_df


if __name__ == "__main__":
    relig_raw_df = Data.get_relig_df()
    relig_raw_df.to_csv("data/religion_base.csv")
    relig_df = Data.enrich_relig_df(relig_raw_df)
    # print(relig_df.head(10))
    relig_df.to_csv("data/religion_enriched.csv")
    pop_df = Data.get_pop_df()
    pop_df.to_csv(Data.POP_PROCESSED_PATH)
    # print(pop_df.head(20))

    total_df = Data.join_tables(pop_df, relig_df)
    total_df.to_csv("data/pop_relig_inner.csv")



    
