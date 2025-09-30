import pandas as pd

df = pd.read_csv("data\cleaned_pop_gdp_relig_df.csv")
df_wdi = pd.read_csv("data\cleanWDI.csv")

# print(list(df_wdi))

df_wdi["alpha_2"] = df_wdi["Country Code"]
df_wdi.drop(columns=["Country Code"], inplace=True)

df_wdi["name"] = df_wdi["Country Name"]
df_wdi.drop(columns=["Country Name"], inplace=True)

df_wdi["year"] = df_wdi["Time"]
df_wdi.drop(columns=["Time"], inplace=True)

combined_df = pd.merge(df, df_wdi, on=["alpha_2", "year"], how="outer")

combined_df["name"] = combined_df.apply(
        lambda row: row["name_y"] if pd.notna(row["name_y"]) else row["name_x"],
        axis=1
    )
combined_df.drop(columns=["name_x", "name_y"], inplace=True)

desired_order = ['name', 'alpha_2', 'iso_2', 'abb', 'year', 'population', 'pop', 'GDP', 'christian', 'islam', 'buddhist', 'judaism', 'nonrelig', 'other', 'Fertility rate, total (births per woman) [SP.DYN.TFRT.IN]', 'Birth rate, crude (per 1,000 people) [SP.DYN.CBRT.IN]', 'Death rate, crude (per 1,000 people) [SP.DYN.CDRT.IN]', 'Life expectancy at birth, total (years) [SP.DYN.LE00.IN]', 'Net migration [SM.POP.NETM]', 'Individuals using the Internet (% of population) [IT.NET.USER.ZS]', 'Gini index [SI.POV.GINI]', 'Human capital index (HCI) (scale 0-1) [HD.HCI.OVRL]', 'School enrollment, secondary (gross), gender parity index (GPI) [SE.ENR.SECO.FM.ZS]', 'Literacy rate, youth (ages 15-24), gender parity index (GPI) [SE.ADT.1524.LT.FM.ZS]', 'Urban population (% of total population) [SP.URB.TOTL.IN.ZS]', 'Mortality rate, infant (per 1,000 live births) [SP.DYN.IMRT.IN]', 'Current health expenditure per capita, PPP (current international $) [SH.XPD.CHEX.PP.CD]', 'Population, female (% of total population) [SP.POP.TOTL.FE.ZS]', 'Population, male (% of total population) [SP.POP.TOTL.MA.ZS]', 'Population, total [SP.POP.TOTL]']
combined_df = combined_df[[col for col in desired_order if col in combined_df.columns]]
combined_df = combined_df.copy()
combined_df.to_csv("data/cleaned_all_data.csv")