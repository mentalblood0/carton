import dataclasses
import sqlite3
import typing

from ..Cursor import Cursor
from ..Database import Database


@dataclasses.dataclass
class SqliteCursor(Cursor):
    cursor: sqlite3.Cursor

    def execute(self, query: str, arguments: typing.Tuple[typing.Any, ...] = ()):
        print(query, arguments)
        return self.cursor.execute(query, arguments)

    def executemany(self, query: str, arguments: typing.Union[typing.List[typing.Tuple[typing.Any, ...]], None] = None):
        return self.cursor.executemany(query, arguments or [])


@dataclasses.dataclass
class Sqlite(Database):
    connection: sqlite3.Connection

    def cursor(self):
        return SqliteCursor(self.connection.cursor())

    def commit(self):
        self.connection.commit()

    def create(self):
        cursor = self.cursor()
        cursor.execute("create table if not exists keys(id integer primary key autoincrement, key text unique)", ())
        cursor.execute("create index if not exists keys_key on keys(key)", ())
        cursor.execute(
            "create table if not exists carton(id integer primary key autoincrement,"
            "time datetime default(datetime('now')) not null,package integer not null,"
            "key integer not null,value text,actual integer default(1) not null,foreign key(key) references keys(id))",
            (),
        )
        cursor.execute("create index if not exists carton_time on carton(time)", ())
        cursor.execute("create index if not exists carton_actual_key_value on carton(key,value) where actual=true", ())
        cursor.execute("create index if not exists carton_package_key_value on carton(package,key,value)", ())
        self.commit()
