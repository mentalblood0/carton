import dataclasses
import functools
import itertools
import typing


@dataclasses.dataclass
class Log:
    execute: typing.Callable[[str], typing.Any]
    execute_for_enum: typing.Callable[[str], typing.Any]

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
            "time datetime default(datetime('now')) not null,"
            "id bigint not null,"
            "key integer not null,"
            "value text,"
            "foreign key(key) references keys(i))"
        )
        self.execute("create index if not exists log_time on log(time)")
        self.execute("create index if not exists log_id on log(id)")
        self.execute("create index if not exists log_value on log(value)")
        self.execute("create index if not exists log_i_key on log(i, key)")

    def insert(self, packages: typing.Iterable[tuple[int, dict[str, str]]]):
        for b in itertools.batched(
            ((p[0], key, str(value)) for p in packages for key, value in p[1].items()),
            10**5,
        ):
            self.execute(
                "insert into log(id,key,value) values "
                + ",".join(f"({e[0]},{self.key_id(e[1])},'{e[2]}')" for e in b)
            )

    def __hash__(self):
        return hash(self.execute)

    @functools.cache
    def key_id(self, key: str) -> int:
        while True:
            try:
                return next(
                    self.execute_for_enum(f"select i from keys where key='{key}'")
                )[0]
            except StopIteration:
                self.execute_for_enum(
                    f"insert into keys(key) values ('{key}') on conflict(key) do nothing"
                )

    @functools.cache
    def id_key(self, id: int) -> str:
        return next(self.execute_for_enum(f"select key from keys where i={id}"))[0]

    def select(
        self,
        present: dict[str, str | typing.Literal[True]],
        absent: dict[str, str | typing.Literal[True]] = {},
        get: set[str] = set(),
    ):
        query = "select i,id,key,value from log where " + " and ".join(
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
            )
        )
        if len(get):
            query += "and (" + " or ".join(f"key={self.key_id(k)}" for k in get) + ")"
        current = {}
        for row in self.execute(query + " order by id,i"):
            if "id" in current and current["id"] != row[1]:
                yield current
                current = {}
            current |= {"id": row[1], self.id_key(row[2]): row[3]}
        yield current
