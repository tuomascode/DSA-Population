import json
import database
from database import models

from pycountry import countries

custom_map = {
    "Antigua & Barbuda": "Antigua and Barbuda",
    "Cape Verde": "Cabo Verde",
    "Niger": "Republic of the Niger",
    "Czechoslovakia": "Czechoslovakia",  # historical, pycountry will fail
    "Democratic Republic of the Congo": "Congo, The Democratic Republic of the",
    "East Timor": "Timor-Leste",
    "German Democratic Republic": "East Germany",  # not in pycountry
    "German Federal Republic": "West Germany",     # not in pycountry
    "Ivory Coast": "Côte d'Ivoire",
    "Republic of Vietnam": "South Vietnam",        # not in pycountry
    "St. Kitts and Nevis": "Saint Kitts and Nevis",
    "St. Lucia": "Saint Lucia",
    "St. Vincent and the Grenadines": "Saint Vincent and the Grenadines",
    "Swaziland": "Eswatini",
    "Turkey": "Türkiye",
    "Yemen Arab Republic": "North Yemen",          # not in pycountry
    "Yemen People's Republic": "South Yemen",      # not in pycountry
    "Yugoslavia": "Yugoslavia"                     # not in pycountry
}


def get_country(name: str):
    if name in custom_map:
        name = custom_map[name]
    return countries.search_fuzzy(name)[0]

def code_based_get(alpha3: str):
    rs = countries.get(alpha_3=alpha3)
    if rs:
        return rs
    rs = lambda: None
    setattr(rs, "alpha_2", "XX")
    if alpha3 == "XKX": # Bypass custom Kosovo code
        setattr(rs, "alpha_2", "XK")
    return rs


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
