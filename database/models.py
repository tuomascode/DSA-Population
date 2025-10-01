"""Layouts the top-level sqlalchemy schema for the database."""
import sqlalchemy
from sqlalchemy import orm

class Base(orm.DeclarativeBase):
    type_annotation_map = {
        int: sqlalchemy.INTEGER,
        str: sqlalchemy.TEXT,
        float: sqlalchemy.REAL
    }


class DataEntry(Base):
    __tablename__ = "data"
    __table_args__ = {"sqlite_strict": True, "comment": "Core Time-Serialized data entries"}
    country_code: orm.Mapped[str] = orm.mapped_column(sqlalchemy.ForeignKey("countries.country_code"),
                                                      comment="ISO 3166-1 alpha-2", primary_key=True)
    year: orm.Mapped[int] = orm.mapped_column(primary_key=True, comment="Statistic for specified year.")

    # General Data
    gdp: orm.Mapped[float|None] = orm.mapped_column(comment="GDP at the time")
    population: orm.Mapped[int|None] = orm.mapped_column(comment="Population at the time")
    female: orm.Mapped[float | None] = orm.mapped_column(comment="Percentage of female population")
    male: orm.Mapped[float | None] = orm.mapped_column(comment="Percentage of male population")

    # Direct Predictors
    fertility: orm.Mapped[float | None] = orm.mapped_column(comment="Fertility rate, total (births per woman)")
    birth_rate: orm.Mapped[float | None] = orm.mapped_column(comment="Birth rate, crude (per 1,000 people)")
    death_rate: orm.Mapped[float | None] = orm.mapped_column(comment="Death rate, crude (per 1,000 people)")
    life_expectancy: orm.Mapped[float | None] = orm.mapped_column(comment="Life expectancy at birth, total (years)")
    migration: orm.Mapped[int | None] = orm.mapped_column(comment="Net Migration")
    infant_mortality: orm.Mapped[float | None] = orm.mapped_column(comment="Mortality rate, infant (per 1,000 live births)")
    health_spend: orm.Mapped[float | None] = orm.mapped_column(comment="Current health expenditure per capita, PPP (current international $)")

    # Religion Data
    christian: orm.Mapped[float|None] = orm.mapped_column(comment="Percentage of christian population")
    islam: orm.Mapped[float|None] = orm.mapped_column(comment="Percentage of islam population")
    buddhist: orm.Mapped[float|None] = orm.mapped_column(comment="Percentage of buddhist population")
    judaism: orm.Mapped[float|None] = orm.mapped_column(comment="Percentage of judaism population")
    nonreligious: orm.Mapped[float|None] = orm.mapped_column(comment="Percentage of nonreligious population")
    other_religions: orm.Mapped[float|None] = orm.mapped_column(comment="Percentage of other religious population")

    # Other
    internet: orm.Mapped[float|None] = orm.mapped_column(comment="Individuals using the Internet (% of population)")
    gini: orm.Mapped[float|None] = orm.mapped_column(comment="Gini index")
    hci: orm.Mapped[float|None] = orm.mapped_column(comment="Human Capital Index")
    enrollment: orm.Mapped[float|None] = orm.mapped_column(comment="School enrollment, secondary (gross), gender parity index (GPI)")
    literacy_rate: orm.Mapped[float|None] = orm.mapped_column(comment="Literacy rate, youth (ages 15-24), gender parity index (GPI)")
    urban_pop: orm.Mapped[float|None] = orm.mapped_column(comment="Urban population (% of total population)")

    # Relationship
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