import dataclasses
import datetime
import itertools
import typing


@dataclasses.dataclass
class Log:
    execute: typing.Callable[[str], list[tuple[typing.Any, ...]]]

    def __post_init__(self):
        self.execute(
            "create table if not exists log("
            "time timestamp without timezone not null, "
            "id bigint not null, "
            "key varchar(32) not null, "
            "value varchar(50))"
        )
        self.execute("create index if not exists log_time on log(time)")
        self.execute("create index if not exists log_id on log(id)")
        self.execute("create index if not exists log_value on log(value)")
        self.execute("create index if not exists log_id_key on log(id, key)")

    @classmethod
    def entries(cls, id: int, pairs: dict[str, typing.Any]):
        now = datetime.datetime.now()
        return ((now, id, key, str(value)) for key, value in pairs.items())

    def insert(self, entries: typing.Iterable[tuple[datetime.datetime, int, str, str]]):
        for b in itertools.batched(entries, 10**5):
            self.execute(
                "insert into log(time, id, key, value) values "
                + ",".join(f"({e[0].timestamp()},{e[1]},'{e[2]}','{e[3]}')" for e in b)
            )

    def select(
        self,
        present: dict[str, str | typing.Literal[True]],
        absent: dict[str, str | typing.Literal[True]],
        get: set[str],
    ):
        return [
            {"id": id} | {row[2]: row[3] for row in sorted(group, key=lambda g: g[0])}
            for id, group in itertools.groupby(
                self.execute(
                    "select * from log where "
                    + " and ".join(
                        itertools.chain(
                            (
                                f"id in (select id from log where key='{key}'"
                                + (f" and value='{value}')" if value != True else ")")
                                for key, value in present.items()
                            ),
                            (
                                f"id not in (select id from log where key='{key}'"
                                + (f" and value='{value}')" if value != True else ")")
                                for key, value in absent.items()
                            ),
                            ["(" + " or ".join(f"key='{key}'" for key in get) + ")"],
                        )
                    )
                ),
                lambda row: row[1],
            )
        ]
