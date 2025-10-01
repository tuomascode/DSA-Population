"""
This script imports time-series country data from a CSV file into the database.

It maps the specific CSV headers to the `DataEntry` model fields,
handles data type conversion, and skips rows with missing primary key data.

Usage:
    python importer.py /path/to/your/data.csv
"""

import argparse
import csv
import logging
import pathlib
from typing import Any, Type, Optional

from database import models, sessions

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Data Mapping Configuration ---

# Maps the exact CSV header strings to the corresponding attribute in the DataEntry model.
HEADER_MAP = {
    'alpha_2': 'country_code',
    'year': 'year',
    'GDP': 'gdp',
    'christian': 'christian',
    'islam': 'islam',
    'buddhist': 'buddhist',
    'judaism': 'judaism',
    'nonrelig': 'nonreligious',
    'other': 'other_religions',
    "Fertility rate, total (births per woman) [SP.DYN.TFRT.IN]": 'fertility',
    "Birth rate, crude (per 1,000 people) [SP.DYN.CBRT.IN]": 'birth_rate',
    "Death rate, crude (per 1,000 people) [SP.DYN.CDRT.IN]": 'death_rate',
    "Life expectancy at birth, total (years) [SP.DYN.LE00.IN]": 'life_expectancy',
    'Net migration [SM.POP.NETM]': 'migration',
    'Individuals using the Internet (% of population) [IT.NET.USER.ZS]': 'internet',
    'Gini index [SI.POV.GINI]': 'gini',
    'Human capital index (HCI) (scale 0-1) [HD.HCI.OVRL]': 'hci',
    "School enrollment, secondary (gross), gender parity index (GPI) [SE.ENR.SECO.FM.ZS]": 'enrollment',
    "Literacy rate, youth (ages 15-24), gender parity index (GPI) [SE.ADT.1524.LT.FM.ZS]": 'literacy_rate',
    "Urban population (% of total population) [SP.URB.TOTL.IN.ZS]": 'urban_pop',
    "Mortality rate, infant (per 1,000 live births) [SP.DYN.IMRT.IN]": 'infant_mortality',
    "Current health expenditure per capita, PPP (current international $) [SH.XPD.CHEX.PP.CD]": 'health_spend',
    "Population, female (% of total population) [SP.POP.TOTL.FE.ZS]": 'female',
    "Population, male (% of total population) [SP.POP.TOTL.MA.ZS]": 'male',
    # "Population, total [SP.POP.TOTL]" is handled by custom logic below
}

# Maps the model attributes to their expected Python types for robust casting.
TYPE_MAP = {
    'country_code': str,
    'year': int,
    'gdp': float,
    'population': int,
    'female': float,
    'male': float,
    'fertility': float,
    'birth_rate': float,
    'death_rate': float,
    'life_expectancy': float,
    'migration': int,
    'infant_mortality': float,
    'health_spend': float,
    'christian': float,
    'islam': float,
    'buddhist': float,
    'judaism': float,
    'nonreligious': float,
    'other_religions': float,
    'internet': float,
    'gini': float,
    'hci': float,
    'enrollment': float,
    'literacy_rate': float,
    'urban_pop': float,
}


def _safe_cast(value: Optional[str], cast_to: Type) -> Optional[Any]:
    """
    Safely casts a string value to a specified type. Handles null-like values,
    commas in numbers, and float-to-int conversion.
    """
    if value is None or value.strip().lower() in ('', 'na', 'n/a', '..', 'null'):
        return None

    cleaned_value = value.strip().replace(',', '')
    if not cleaned_value:
        return None

    try:
        if cast_to is int:
            # Cast to float first to handle strings like "1234.0", then to int.
            return int(float(cleaned_value))
        return cast_to(cleaned_value)
    except (ValueError, TypeError):
        logging.debug(f"Could not cast '{value}' to {cast_to.__name__}. Returning None.")
        return None


def import_data(csv_path: pathlib.Path):
    """
    Imports data from the specified CSV file into the database.
    """
    if not csv_path.exists():
        logging.error(f"File not found: {csv_path}")
        return

    logging.info("Fetching existing country codes from the database...")
    try:
        with sessions() as session:
            results = session.query(models.Country.country_code).all()
            valid_country_codes = {code for (code,) in results}
        logging.info(f"Found {len(valid_country_codes)} countries in the database.")
    except Exception as e:
        logging.error(f"Could not connect to database to fetch country codes: {e}", exc_info=True)
        logging.error("Aborting import. Please ensure the database is set up and Country data is imported.")
        return

    logging.info(f"Starting import from {csv_path}...")

    unique_entries = {}

    try:
        with open(csv_path, mode='r', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)

            for i, row in enumerate(reader, start=2):

                raw_country_code = row.get('alpha_2')
                country_code = None
                if raw_country_code and raw_country_code.strip():
                    country_code = raw_country_code.strip().upper()

                if not country_code:
                    raw_name = row.get('name')
                    if raw_name and raw_name.strip().lower() == 'namibia':
                        country_code = 'NA'
                        logging.debug(f"Row {i}: Used fallback for Namibia based on name to set country code 'NA'.")

                year = _safe_cast(row.get('year'), int)

                if not country_code or not year:
                    if any(row.values()):
                        logging.warning(f"Skipping row {i}: Missing primary key data (country_code or year).")
                    continue

                if country_code not in valid_country_codes:
                    logging.warning(f"Skipping row {i}: Code '{country_code}' not in database.")
                    continue

                entry_data = {'country_code': country_code, 'year': year}

                # Process all mapped fields
                for csv_header, model_attr in HEADER_MAP.items():
                    if model_attr in ('country_code', 'year'):
                        continue
                    entry_data[model_attr] = _safe_cast(row.get(csv_header), TYPE_MAP[model_attr])

                # --- Handle Population with specified fallback logic ---
                population_value = _safe_cast(row.get("Population, total [SP.POP.TOTL]"), int)
                if population_value is None:
                    population_value = _safe_cast(row.get("population"), int)
                if population_value is None:
                    population_value = _safe_cast(row.get("pop"), int)
                entry_data['population'] = population_value

                composite_key = (country_code, year)
                if composite_key in unique_entries:
                    logging.debug(f"Duplicate entry for {composite_key} at row {i} will overwrite previous entry.")
                unique_entries[composite_key] = entry_data

    except Exception as e:
        logging.error(f"Error while reading CSV: {e}", exc_info=True)
        return

    if not unique_entries:
        logging.warning("No valid data found to import.")
        return

    logging.info(f"Processed CSV. Found {len(unique_entries)} valid records to import.")

    entries_to_add = [models.DataEntry(**data) for data in unique_entries.values()]

    try:
        with sessions.begin() as session:
            logging.info("Database session started. Adding all entries...")
            session.add_all(entries_to_add)
            logging.info("Committing transaction...")
        logging.info("Import completed successfully.")
    except Exception as e:
        logging.error(f"Database error during transaction: {e}", exc_info=True)
        logging.error("Transaction rolled back. No data was saved.")


def main():
    """Main function to parse command-line arguments and run the importer."""
    parser = argparse.ArgumentParser(
        description="A script to import country data from a CSV file into the database.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "csv_file",
        type=pathlib.Path,
        help="Path to the CSV file to import."
    )
    args = parser.parse_args()

    import_data(args.csv_file)


if __name__ == "__main__":
    main()