import sqlite3

import pytest

from .. import databases
from ..Carton import Carton


@pytest.fixture()
def carton():
    db = databases.Sqlite(sqlite3.connect(":memory:"))
    cursor = db.cursor()
    cursor.execute("drop table if exists sentences")
    cursor.execute("drop table if exists predicates")
    db.commit()
    return Carton(db)
