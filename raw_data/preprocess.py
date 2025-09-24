import pandas as pd
from scipy.interpolate import PchipInterpolator
from database.utils import code_based_get


def interpolate_series(s, years, method="pchip"):
    notna = s.dropna()
    if len(notna) < 2:
        return s
    x, y = years[notna.index], notna.values

    if method == "pchip":
        interp = PchipInterpolator(x, y, extrapolate=False)  # important!
        s.loc[s.index] = interp(years)  # align to years
        return s
    elif method == "linear":
        return s.interpolate(method="linear", limit_area="inside")
    return s

def clean_and_tag(df):
    df.drop("Time Code", axis=1, inplace=True)
    df.dropna(subset=["Country Code"], inplace=True)
    df["Country Code"] = df["Country Code"].apply(lambda x: code_based_get(x).alpha_2)
    df.drop(data[data["Country Code"] == "XXX"].index, inplace=True)
    return df


# === Load & clean ===
with open("ff9f9047-7a2a-4f98-a0a5-192c04cbc195_Data.csv") as csvfile:
    data = pd.read_csv(csvfile)

data = clean_and_tag(data)

d_binds: dict[str, type] = {"Country Name": str, "Country Code": str, "Time": int}
for col in data.columns:
    if d_binds.get(col, None) is None:
        d_binds[col] = float

data = data.astype(d_binds)

# Make sure Time is sorted & usable as index for interpolation
data.sort_values(["Country Code", "Time"], inplace=True)

# === Apply interpolation per country & column ===
# Smooth indicators (use PCHIP)
smooth_cols = [
    "Fertility rate, total (births per woman) [SP.DYN.TFRT.IN]",
    "Birth rate, crude (per 1,000 people) [SP.DYN.CBRT.IN]",
    "Death rate, crude (per 1,000 people) [SP.DYN.CDRT.IN]",
    "Life expectancy at birth, total (years) [SP.DYN.LE00.IN]",
    "Individuals using the Internet (% of population) [IT.NET.USER.ZS]",
    "Human capital index (HCI) (scale 0-1) [HD.HCI.OVRL]",
    "School enrollment, secondary (gross), gender parity index (GPI) [SE.ENR.SECO.FM.ZS]",
    "Literacy rate, youth (ages 15-24), gender parity index (GPI) [SE.ADT.1524.LT.FM.ZS]",
    "Urban population (% of total population) [SP.URB.TOTL.IN.ZS]",
    "Mortality rate, infant (per 1,000 live births) [SP.DYN.IMRT.IN]"
]

# Jagged indicators (use Linear)
jagged_cols = [
    "Net migration [SM.POP.NETM]",
    "Gini index [SI.POV.GINI]",
    "Current health expenditure per capita, PPP (current international $) [SH.XPD.CHEX.PP.CD]"
]


edge_fill_cols = [
    "Life expectancy at birth, total (years) [SP.DYN.LE00.IN]",
    "Urban population (% of total population) [SP.URB.TOTL.IN.ZS]",
    "Literacy rate, youth (ages 15-24), gender parity index (GPI) [SE.ADT.1524.LT.FM.ZS]",
    "School enrollment, secondary (gross), gender parity index (GPI) [SE.ENR.SECO.FM.ZS]",
    "Human capital index (HCI) (scale 0-1) [HD.HCI.OVRL]",
]

def fill_group(g):
    g = g.reset_index(drop=True)
    years = g["Time"].values

    for col in smooth_cols:
        if col in g:
            g[col] = interpolate_series(g[col], years, method="pchip")
            if col in edge_fill_cols:
                g[col] = g[col].ffill().bfill()

    for col in jagged_cols:
        if col in g:
            g[col] = interpolate_series(g[col], years, method="linear")
            # leave edges NA for jagged indicators
    return g


data = data.groupby("Country Code").apply(fill_group)
data = data.astype({"Net migration [SM.POP.NETM]": int})

# === Save cleaned dataset ===
with open("../data/cleanWDI.csv", "w") as csvfile:
    data.to_csv(csvfile, index=False, lineterminator="\n")
