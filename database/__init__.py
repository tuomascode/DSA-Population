import pathlib

from sqlalchemy import create_engine, event, Engine, orm

import models

# Get the directory of the current file
BASE_DIR = pathlib.Path(__file__).parent
# Construct the path to the database file within the same directory
DB_PATH =  BASE_DIR / "data.db"
# Create the SQLite connection string
DATABASE_URL = f"sqlite:///{DB_PATH.resolve()}"

# Create the database engine
engine = create_engine(
    DATABASE_URL, 
    connect_args={"autocommit": False}
)
sessions = orm.sessionmaker(bind=engine)


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    """
    Enforces foreign key constraints for SQLite connections.
    """
    dbapi_connection.autocommit = True
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()
    dbapi_connection.autocommit = False


models.Base.metadata.create_all(bind=engine)
