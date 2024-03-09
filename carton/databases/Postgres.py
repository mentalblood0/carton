import dataclasses
import typing

from ..Cursor import Cursor
from ..Database import Database


@dataclasses.dataclass
class PostgresCursor(Cursor):
    cursor: typing.Any

    def execute(self, query: str, arguments: typing.Tuple[typing.Any, ...] = ()):
        self.cursor.execute(query.replace("?", "%s"), arguments)
        return (r for r in self.cursor)

    def executemany(self, query: str, arguments: typing.Union[typing.List[typing.Tuple[typing.Any, ...]], None] = None):
        self.cursor.executemany(query.replace("?", "%s"), arguments or [])
        return (r for r in self.cursor)


@dataclasses.dataclass
class Postgres(Database):
    connection: typing.Any

    def cursor(self):
        return PostgresCursor(self.connection.cursor())

    def commit(self):
        self.connection.commit()

    def create(self):
        cursor = self.cursor()
        cursor.execute("create table if not exists predicates(id bigserial primary key, predicate text unique)", ())
        cursor.execute("create index if not exists predicates_predicate on predicates(predicate)", ())
        cursor.execute(
            "create table if not exists sentences(id bigserial primary key,"
            "time timestamp default(now() at time zone 'utc') not null,subject bigint not null,"
            "predicate bigint not null,actual boolean default(true) not null,"
            "foreign key(predicate) references predicates(id))",
            (),
        )
        cursor.execute("create index if not exists sentences_time on sentences(time)", ())
        cursor.execute(
            "create index if not exists sentences_actual_predicate on sentences(predicate) where actual=true", ()
        )
        cursor.execute("create index if not exists sentences_subject_actual on sentences(subject,actual)", ())
        self.commit()
