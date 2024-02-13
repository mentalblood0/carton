import dataclasses
import operator
import typing


@dataclasses.dataclass(unsafe_hash=True, frozen=True, kw_only=True)
class Log:
    execute: typing.Callable[[str, tuple[typing.Any, ...]], typing.Any]
    executemany: typing.Callable[[str, typing.Iterable[tuple[typing.Any, ...]]], typing.Any]
    execute_enum: typing.Callable[[str, tuple[typing.Any, ...]], typing.Any]
    integer: str = "integer"
    now: str = "datetime('now')"
    key_id_cache: dict[str, int] = dataclasses.field(default_factory=dict)
    id_key_cache: dict[int, str] = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        self.execute("create table if not exists keys(id integer primary key autoincrement,key text unique)", ())
        self.execute("create index if not exists keys_key on keys(key)", ())
        self.execute(
            "create table if not exists log("
            f"id {self.integer} primary key autoincrement,"
            f"time datetime default({self.now}) not null,"
            f"package {self.integer} not null,"
            "key integer not null,"
            "value text,"
            "foreign key(key) references keys(id))",
            (),
        )
        self.execute("create index if not exists log_time on log(time)", ())
        self.execute("create index if not exists log_package on log(package)", ())
        self.execute("create index if not exists log_value on log(value)", ())
        self.execute("create index if not exists log_id_key on log(id,key)", ())

    def insert(self, packages: typing.Iterable[tuple[int | None, dict[str, str]]]):
        buf = []
        for p in filter(operator.itemgetter(1), packages):
            if p[0] is not None:
                buf.extend((p[0], self.key_id(k), v) for k, v in p[1].items())
            else:
                k_v = list(p[1].items())
                p_id = self.execute(
                    "insert into log(package,key,value)values(coalesce((select max(package)from log),-1)+1,?,?)"
                    "returning package",
                    (
                        self.key_id(k_v[0][0]),
                        k_v[0][1],
                    ),
                ).__next__()[0]
                buf.extend((p_id, self.key_id(e[0]), e[1]) for e in k_v[1:])
        self.executemany("insert into log(package,key,value)values(?,?,?)", buf)

    def key_id(self, key: str) -> int:
        if key not in self.key_id_cache:
            try:
                self.key_id_cache[key] = next(self.execute_enum("select id from keys where key=?", (key,)))[0]
            except StopIteration:
                self.key_id_cache[key] = next(self.execute_enum("insert into keys(key)values(?)returning *", (key,)))[0]
        return self.key_id_cache[key]

    def id_key(self, i: int) -> str:
        if i not in self.id_key_cache:
            self.id_key_cache[i] = next(self.execute_enum("select key from keys where id=?", (i,)))[0]
        return self.id_key_cache[i]

    def select(
        self,
        present: dict[str, str | typing.Literal[True]],
        absent: dict[str, str | typing.Literal[True]] | None = None,
        get: set[str] | None = None,
    ):
        query = "select package,key,value from log where " + " and ".join(
            f"package {clause} (select package from log where key={self.key_id(key)}"
            + (f" and value='{value}')" if value is not True else ")")
            for clause, d in (("in", present), ("not in", absent))
            for key, value in (d or {}).items()
        )
        if get:
            query += "and(" + " or ".join(f"key={self.key_id(k)}" for k in get) + ")"
        current = {}
        for row in self.execute(query + "order by package,id", ()):
            if "package" in current and current["package"] != row[0]:
                yield current
                current = {}
            current |= {"package": row[0], self.id_key(row[1]): row[2]}
        yield current
