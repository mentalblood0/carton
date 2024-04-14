import sqlite3

import pytest

from .. import databases
from ..Carton import Carton


@pytest.fixture
def carton():
    db = databases.Sqlite(sqlite3.connect(":memory:"))
    return Carton(db)
