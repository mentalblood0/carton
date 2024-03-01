import sqlite3

# import psycopg2
import pytest

from .. import databases
from ..Carton import Carton


@pytest.fixture()
def carton():
    # db = databases.Postgres(psycopg2.connect(user="postgres", port=5432))
    db = databases.Sqlite(sqlite3.connect(":memory:"))
    # db = databases.Sqlite(sqlite3.connect("test.db"))
    cursor = db.cursor()
    cursor.execute("drop table if exists carton")
    cursor.execute("drop table if exists keys")
    db.commit()
    return Carton(db)
