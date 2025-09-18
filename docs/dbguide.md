# **SQLAlchemy Usage Guide: Getting Started**

This guide is for anyone new to SQLAlchemy. We'll use your existing file structure to show you how to work with the database using simple Python code. Think of SQLAlchemy as a way to use normal Python objects to store and retrieve data from your database, without writing raw SQL.

### **1\. Understanding the File Structure**

* **database/\_\_init\_\_.py**: This file handles the "plumbing." It's what connects your Python code to the database file itself (data.db). The sessions object it provides is the key to all your database interactions.  
* **database/models.py**: This is where you define your data. The Country and DataEntry classes are like blueprints for the records you'll be storing. They tell SQLAlchemy what kind of data each record holds. We'll focus on the DataEntry model, which has been updated to allow gdp and population to be optional (you can store a value or leave it as None).  
* **database/utils.py**: This file contains handy functions that use the other two files. For example, setup\_countrydb() shows how to load initial data into the Country table.

### **2\. Basic Database Operations with DataEntry**

The four most common things you'll do with a database are Create, Read, Update, and Delete. We'll call these "CRUD" operations. For each step, you'll first need to get a "session," which is your temporary workspace with the database.

#### **Create: Adding a New Data Record**

To add a new record, you just create a new DataEntry object and add it to your session. When the session is committed, SQLAlchemy writes it to the database.
```python
from database import sessions  
from database.models import DataEntry  
from sqlalchemy.exc import IntegrityError
# The `with sessions.begin()` line is a safe way to handle a session.  
# It automatically saves your changes and closes the session.  
with sessions.begin() as session:  
    try:  
        # Create a new DataEntry record with all data  
        record_with_data = DataEntry(  
            country_code="US",  
            year=2022,  
            gdp=25460000000000,  
            population=333000000  
        )  
        session.add(record_with_data)

        # Create another record, this time without GDP data  
        record_without_gdp = DataEntry(  
            country_code="CA",  
            year=2023,  
            population=40000000  
            # gdp is not provided, so it will be None  
        )  
        session.add(record_without_gdp)

        print("New data entries created successfully!")

    except IntegrityError as e:  
        print(f"Error: A record with that year and country code may already exist. {e}")
```
#### **Read: Finding Existing Data**

Reading data is the most common operation. You'll use the select() function to build a query, and then session.scalars() to run it and get the results as a list of Python objects.
```python
from database import sessions  
from database.models import DataEntry  
from sqlalchemy import select

# Use a context manager for the session  
with sessions() as session:  
    # Get all DataEntry records for a specific country  
    us_records = session.scalars(  
        select(DataEntry).where(DataEntry.country_code == "US")  
    ).all()  
    print(f"Found {len(us_records)} records for the United States.")

    # Find records where GDP is not available (is None)  
    unknown_gdp_records = session.scalars(  
        select(DataEntry).where(DataEntry.gdp == None)  
    ).all()  
    print(f"Found {len(unknown_gdp_records)} records with unknown GDP.")

    # Find records with a population greater than 1 billion  
    large_population_records = session.scalars(  
        select(DataEntry).where(DataEntry.population > 1000000000)  
    ).all()  
    for record in large_population_records:  
        print(f"  \- Record found for year {record.year} with population {record.population}")
```
#### **Update: Modifying a Record**

To change an existing record, you first "read" it from the database, then simply change its properties in your Python code. The changes are saved when the session is committed.
```python
from database import sessions  
from database.models import DataEntry  
from sqlalchemy import select

with sessions.begin() as session:  
    # Find the data record to update  
    record_to_update = session.scalars(  
        select(DataEntry).where(DataEntry.country_code == "CA", DataEntry.year == 2023)  
    ).first()

    if record_to_update:  
        print(f"Updating population for record from {record_to_update.population}...")  
        # Change the population value  
        record_to_update.population = 41000000  
        print("Update successful.")  
    else:  
        print("Record for Canada in 2023 not found.")
```
#### **Delete: Removing a Record**

Deleting is just as simple. You find the record you want to remove, then call session.delete() on it.
```python
from database import sessions  
from database.models import DataEntry  
from sqlalchemy import select

with sessions.begin() as session:  
    # Find the data entry to delete  
    entry_to_delete = session.scalars(  
        select(DataEntry).where(DataEntry.country_code == "CA", DataEntry.year == 2023)  
    ).first()

    if entry_to_delete:  
        print(f"Deleting data entry for {entry_to_delete.country_code} in {entry_to_delete.year}...")  
        session.delete(entry_to_delete)  
        print("Deletion successful.")  
    else:  
        print("Data entry not found.")  
```