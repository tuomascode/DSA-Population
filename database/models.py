"""Layouts the top-level sqlalchemy schema for the database."""
import datetime

import sqlalchemy
from sqlalchemy import orm

class Base(orm.DeclarativeBase):
    pass

class DataEntry(Base):
    __tablename__ = "data"
    __table_args__ = {"sqlite_strict": True, "comment": "Core Time-Serialized data entries"}
    year: orm.Mapped[int] = orm.mapped_column(primary_key=True, comment="Statistic for specified year.")
    gdp: orm.Mapped[int] = orm.mapped_column(comment="GDP at the time")
    population: orm.Mapped[int] = orm.mapped_column(comment="Population at the time")
    country_code: orm.Mapped[str] = orm.mapped_column(sqlalchemy.ForeignKey("countries.country_code"), comment="ISO 3166-1 alpha-2", primary_key=True)

    country: orm.Mapped["Country"] = orm.relationship(back_populates="records")

class Country(Base):
    """Used to declare constants regarding each country itself."""
    __tablename__ = "countries"
    __table_args__ = {"sqlite_strict": True, "comment": "Country records, including all permanent information."}
    country_code: orm.Mapped[str] = orm.mapped_column(primary_key=True, comment="Country code as per ISO 3166-1 alpha-2")
    name: orm.Mapped[str] = orm.mapped_column(nullable=False, comment="Country name.")
    lat: orm.Mapped[float] = orm.mapped_column(nullable=False, comment="Latitude.")
    lng: orm.Mapped[float] = orm.mapped_column(nullable=False, comment="Longitude.")

    records: orm.Mapped[list["DataEntry"]] = orm.relationship(back_populates="country", lazy="selectin")