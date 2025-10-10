import json
import database
from database import models

from pycountry import countries


def get_country(name: str):
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
