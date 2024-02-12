import dataclasses
import functools
import itertools
import typing


@dataclasses.dataclass
class Log:
    execute: typing.Callable[[str], typing.Any]
    execute_enum: typing.Callable[[str], typing.Any]

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
            "id integer not null,"
            "key integer not null,"
            "value text,"
            "foreign key(key) references keys(i))"
        )
        self.execute("create index if not exists log_time on log(time)")
        self.execute("create index if not exists log_id on log(id)")
        self.execute("create index if not exists log_value on log(value)")
        self.execute("create index if not exists log_i_key on log(i, key)")

    def insert(self, packages: typing.Iterable[tuple[int | None, dict[str, str]]]):
        buf = []

        def execute_buffer():
            if buf:
                self.execute(f"insert into log(id,key,value) values" + ",".join(buf))
                buf.clear()

        for p in packages:
            if not p[1]:
                continue
            if p[0] is not None:
                buf.extend(f"({p[0]},{self.key_id(k)},'{v}')" for k, v in p[1].items())
            else:
                rows = [(p[0], k, str(v)) for k, v in p[1].items()]
                id = self.execute(
                    f"insert into log(id,key,value) values "
                    f"(coalesce((select max(id) from log), -1) + 1,{self.key_id(rows[0][1])},'{rows[0][2]}')"
                    "returning id"
                ).__next__()[0]
                buf.extend(f"({id},{self.key_id(r[1])},'{r[2]}')" for r in rows[1:])
        execute_buffer()

    def __hash__(self):
        return hash(self.execute)

    @functools.cache
    def key_id(self, key: str) -> int:
        try:
            return next(self.execute_enum(f"select i from keys where key='{key}'"))[0]
        except StopIteration:
            return self.execute_enum(
                f"insert into keys(key) values ('{key}') returning *"
            ).__next__()[0]

    @functools.cache
    def id_key(self, id: int) -> str:
        return next(self.execute_enum(f"select key from keys where i={id}"))[0]

    def select(
        self,
        present: dict[str, str | typing.Literal[True]],
        absent: dict[str, str | typing.Literal[True]] = {},
        get: set[str] = set(),
    ):
        query = "select id,key,value from log where " + " and ".join(
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
        if get:
            query += "and (" + " or ".join(f"key={self.key_id(k)}" for k in get) + ")"
        current = {}
        for row in self.execute(query + " order by id,i"):
            if "id" in current and current["id"] != row[0]:
                yield current
                current = {}
            current |= {"id": row[0], self.id_key(row[1]): row[2]}
        yield current
