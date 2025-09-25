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
        print("Dropping countries with unreasonably large change")
        changes = sorted([(key, value) for key, value in Data.get_country_pop_max_relative_change(df, "Location", "TPopulation1Jan", "Time").items()], key= lambda x:x[1], reverse=True)
        reasonable_max_change_countries = [i[0] for i in changes if i[1] < 0.1]
        dropped_countries = [i for i in df["Location"].unique() if i not in reasonable_max_change_countries]
        print("Dropped countries due to too big change: ", " ".join(dropped_countries))
        df = df[df["Location"].isin(reasonable_max_change_countries)]
        Data.rename_countries(df)
        df = df[df["Time"] > 1945]
        df = df[df["Time"] < 2025]
        df["TPopulation1Jan"] = (df["TPopulation1Jan"]*1_000).astype(int)
        print("renaming columns")
        column_rename_map = {
            "TPopulation1Jan": "population",
            "Location": "name",
            "Time": "year",
            "ISO2_code": "iso_2"
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
        codes = pd.read_csv(Data.download_file("https://correlatesofwar.org/wp-content/uploads/COW-country-codes.csv")).drop_duplicates("StateAbb")
        print("Resolving country names to COW statename")
        code_map = codes.set_index("StateAbb")["StateNme"]
        relig_df["name"] = relig_df["name"].map(code_map)
        # relig_df = Data.add_a2_values(relig_df)
        print("Base relig data processed")
        return relig_df.sort_values(["name", "year"]).reset_index(drop=True)

    @staticmethod
    def get_countries_with_range_atleast(df, y_range = 50):
        country_year_ranges = (
            df.groupby("name")["year"]
            .agg(["min", "max"])
            .apply(lambda row: [row["min"], row["max"]], axis=1)
            .to_dict()
        )
        c_data = [{"name": country, "range": int(val[1] - val[0])} for country, val in country_year_ranges.items()]
        c_data.sort(key = lambda x: x["range"])
        return [data["name"] for data in c_data if data["range"] >= y_range]

    @staticmethod
    def get_country_pop_max_relative_change(df, c_name = "name", c_pop = "pop", c_year = "year"):
        df_sorted = df.sort_values([c_name, c_year])
        df_sorted["rel_change"] = df_sorted.groupby(c_name)[c_pop].pct_change().abs()
        return (
            df_sorted.groupby(c_name)["rel_change"]
            .max()
            .dropna()
            .to_dict()
        )

    @staticmethod
    def clean_relig_data(df):
        reasonable_range_countries = Data.get_countries_with_range_atleast(df, 50)
        df = df[df["name"].isin(reasonable_range_countries)]
        changes = sorted([(key, value) for key, value in Data.get_country_pop_max_relative_change(df).items()], key= lambda x:x[1], reverse=True)
        reasonable_max_change_countries = [i[0] for i in changes if i[1] < 0.3]
        df = df[df["name"].isin(reasonable_max_change_countries)]
        df = Data.add_a2_values(df)
        return df.sort_values(["name", "year"]).reset_index(drop=True)
        
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
        combined_df = pd.merge(pop_df, relig_df, on=["alpha_2", "year"], how="inner")
        desired_order = [
            "name_x", "name_y", "alpha_2", "iso_2",  "abb", "year", "population", "pop",
            "christian", "islam", "buddhist", "judaism", "nonrelig", "other"
        ]
        combined_df = combined_df[[col for col in desired_order if col in combined_df.columns]]
        df = combined_df.copy()
        df["pop_diff_ratio"] = ((df["pop"] - df["population"]).abs()) / df[["pop", "population"]].min(axis=1)
        df_sorted = df.sort_values("pop_diff_ratio", ascending=False)
        df_unique = df_sorted.drop_duplicates(subset="alpha_2", keep="first").reset_index(drop=True)
        alpha_2s = df_unique[df_unique["pop_diff_ratio"] < 0.2]["alpha_2"].unique()
        combined_df = combined_df[combined_df["alpha_2"].isin(alpha_2s)]
        return combined_df.copy().reset_index(drop=True).sort_values(["alpha_2", "year"])


if __name__ == "__main__":
    if True:
        relig_raw_df = Data.get_relig_df()
        relig_clean_df = Data.clean_relig_data(relig_raw_df)
        relig_rich_df = Data.enrich_relig_df(relig_clean_df)
        relig_rich_df.to_csv("data/cleaned_relig_df.csv")
        pop_df = Data.get_pop_df()
        pop_df.to_csv("data/cleaned_pop_df.csv")
        df = Data.join_tables(pop_df, relig_rich_df)
        df.to_csv("data/cleaned_pop_relig_df.csv")
    else:
        df = pd.read_csv("data/cleaned_pop_relig_df.csv")
