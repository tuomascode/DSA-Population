import json
import database
from database import models

from pycountry import countries

def get_country(name: str):
    # The index is available from countries.search_fuzzy(name)[0].alpha_2
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
