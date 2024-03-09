import dataclasses
import sqlite3
import typing

from ..Cursor import Cursor
from ..Database import Database


@dataclasses.dataclass
class SqliteCursor(Cursor):
    cursor: sqlite3.Cursor

    def execute(self, query: str, arguments: typing.Tuple[typing.Any, ...] = ()):
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
        cursor.execute(
            "create table if not exists predicates(id integer primary key autoincrement, predicate text unique)", ()
        )
        cursor.execute("create index if not exists predicates_predicate on predicates(predicate)", ())
        cursor.execute(
            "create table if not exists sentences(id integer primary key autoincrement,"
            "time datetime default(datetime('now')) not null,subject integer not null,"
            "predicate integer not null,actual integer default(1) not null,"
            "foreign key(predicate) references predicates(id))",
            (),
        )
        cursor.execute("create index if not exists sentences_time on sentences(time)", ())
        cursor.execute(
            "create index if not exists sentences_actual_predicate on sentences(predicate) where actual=true", ()
        )
        cursor.execute("create index if not exists sentences_subject_actual on sentences(subject,actual)", ())
        self.commit()
