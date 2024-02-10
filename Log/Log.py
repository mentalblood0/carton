import dataclasses
import datetime
import itertools
import typing


@dataclasses.dataclass
class Log:
    execute: typing.Callable[[str], list[tuple[typing.Any, ...]]]
    keys_ids: dict[str, int] = dataclasses.field(default_factory=dict)
    ids_keys: dict[int, str] = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        self.execute(
            "create table if not exists keys("
            "i integer primary key autoincrement,"
            "key text unique)"
        )
        self.execute("create index if not exists keys_key on keys(key)")
        self.execute(
            "create table if not exists log("
            "i integer primary key autoincrement,"
            "time timestamp without timezone not null,"
            "id bigint not null,"
            "key integer not null,"
            "value text,"
            "foreign key(key) references keys(i))"
        )
        self.execute("create index if not exists log_time on log(time)")
        self.execute("create index if not exists log_id on log(id)")
        self.execute("create index if not exists log_value on log(value)")
        self.execute("create index if not exists log_i_key on log(i, key)")

    @classmethod
    def entries(cls, id: int, pairs: dict[str, typing.Any]):
        now = datetime.datetime.now()
        return ((now, id, key, str(value)) for key, value in pairs.items())

    def insert(self, entries: typing.Iterable[tuple[datetime.datetime, int, str, str]]):
        for e in entries:
            self.execute(
                f"insert into keys(key) values ('{e[2]}') on conflict(key) do nothing"
            )
            key_enum_id = self.execute(f"select i from keys where key='{e[2]}'")[0][0]
            self.execute(
                "insert into log(time,id,key,value) values "
                + f"({e[0].timestamp()},{e[1]},{key_enum_id},'{e[3]}')"
            )

    def key_id(self, key: str) -> int:
        if not key in self.keys_ids:
            id: int = self.execute(f"select i from keys where key='{key}'")[0][0]
            self.keys_ids[key] = id
            self.ids_keys[id] = key
        return self.keys_ids[key]

    def id_key(self, id: int) -> str:
        if not id in self.ids_keys:
            key: str = self.execute(f"select key from keys where i={id}")[0][0]
            self.keys_ids[key] = id
            self.ids_keys[id] = key
        return self.ids_keys[id]

    def select(
        self,
        present: dict[str, str | typing.Literal[True]],
        absent: dict[str, str | typing.Literal[True]],
        get: set[str],
    ):
        return [
            {"id": id}
            | {self.id_key(row[3]): row[4] for row in sorted(group, key=lambda g: g[0])}
            for id, group in itertools.groupby(
                self.execute(
                    "select * from log where "
                    + " and ".join(
                        itertools.chain(
                            (
                                f"id in (select id from log where key={self.key_id(key)}"
                                + (f" and value='{value}')" if value != True else ")")
                                for key, value in present.items()
                            ),
                            (
                                f"id not in (select id from log where key={self.key_id(key)}"
                                + (f" and value='{value}')" if value != True else ")")
                                for key, value in absent.items()
                            ),
                            [
                                "("
                                + " or ".join(f"key={self.key_id(key)}" for key in get)
                                + ")"
                            ],
                        )
                    )
                ),
                lambda row: row[2],
            )
        ]
