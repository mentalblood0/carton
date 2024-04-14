import sqlite3

import psycopg2
import pytest

from .. import databases
from ..Carton import Carton


@pytest.fixture
def carton():
    # db = databases.Sqlite(sqlite3.connect(":memory:"))
    db = databases.Postgres(psycopg2.connect(database="db1", password="fonofme"))
    cursor = db.cursor()
    cursor.execute("drop table if exists sentences")
    cursor.execute("drop table if exists predicates")
    db.commit()
    return Carton(db)
