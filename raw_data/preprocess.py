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


# === Load & clean ===
with open("d8bf9016-48ec-453e-b74e-ae48b7d39d81_Data.csv") as csvfile:
    data = pd.read_csv(csvfile)

data.drop(["Country Name", "Time Code"], axis=1, inplace=True)
data.dropna(subset=["Country Code"], inplace=True)
data["Country Code"] = data["Country Code"].apply(lambda x: code_based_get(x).alpha_2)
data.drop(data[data["Country Code"] == "XXX"].index, inplace=True)

# Make sure Time is sorted & usable as index for interpolation
data = data.astype({"Time": "int32"})
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

# === Save cleaned dataset ===
with open("cleanWDI.csv", "w") as csvfile:
    data.to_csv(csvfile, index=False, lineterminator="\n")
