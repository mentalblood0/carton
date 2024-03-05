import sqlite3

import pytest

from .. import databases
from ..Carton import Carton


@pytest.fixture()
def carton():
    db = databases.Sqlite(sqlite3.connect(":memory:"))
    cursor = db.cursor()
    cursor.execute("drop table if exists carton")
    cursor.execute("drop table if exists keys")
    db.commit()
    return Carton(db)
