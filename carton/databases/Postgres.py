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
        cursor.execute("create table if not exists keys(id serial primary key, key text unique)", ())
        cursor.execute("create index if not exists keys_key on keys(key)", ())
        cursor.execute(
            "create table if not exists carton(id bigserial primary key,"
            "time timestamp default(now() at time zone 'utc') not null,package bigint not null,"
            "key integer not null,value text,actual boolean default(true) not null,"
            "foreign key(key) references keys(id))",
            (),
        )
        cursor.execute("create index if not exists carton_time on carton(time)", ())
        cursor.execute("create index if not exists carton_actual_key_value on carton(key,value) where actual=true", ())
        cursor.execute("create index if not exists carton_package_key_value on carton(package,key,value)", ())
        self.commit()
