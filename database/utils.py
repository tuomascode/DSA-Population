import json
import database
from database import models

from pycountry import countries

custom_map = {
    "XK": "Kosovo",
    "BHM": "Bahamas",
    "GRN": "Grenada",
    "SVG": "Saint Vincent and the Grenadines",
    "SKN": "Saint Kitts and Nevis",
    "UKG": "United Kingdom",
    "FRN": "France",
    "MNC": "Monaco",
    "SPN": "Spain",
    "GMY": "Germany",
    "GFR": "Germany",
    "GDR": "East Germany",
    "CZR": "Czechia",
    "SNM": "San Marino",
    "MLD": "Maldives",
    "GRG": "Georgia",
    "SWD": "Sweden",
    "EQG": "Equatorial Guinea",
    "CDI": "CI",
    "BFO": "Burkina Faso",
    "DRC": "CD",
    "MZM": "Mozambique",
    "UAE": "United Arab Emirates",
    "KZK": "Kazakhstan",
    "BNG": "Bangladesh",
    "DRV": "North Vietnam",
    "RVN": "South Vietnam",
    "ETM": "Timor-Leste",
}

def get_country(name: str):
    if name in custom_map:
        name = custom_map[name]
    return countries.search_fuzzy(name)[0]

def setup_countrydb():
    print("Setting up country database")
    datb = database.BASE_DIR / ".." / "data" / "countries.json"
    datb = datb.resolve()
    with database.sessions.begin() as session:
        with open(datb, "rb") as f:
            json_data = json.load(f)
        for item in json_data:
            cont = models.Country(country_code=item["iso2"], name=item["name"], lat=item["latitude"], lng=item["longitude"])
            session.add(cont)
    print("Country database setup complete")

if __name__ == "__main__":
    # Don't really wanna overcomplicate it.
    setup_countrydb()