import dataclasses
import functools
import typing


@dataclasses.dataclass(unsafe_hash=True)
class Log:
    execute: typing.Callable[[str], typing.Any]
    execute_enum: typing.Callable[[str], typing.Any]

    def __post_init__(self):
        self.execute(
            "create table if not exists keys("
            "id integer primary key autoincrement,"
            "key text unique)"
        )
        self.execute("create index if not exists keys_key on keys(key)")
        self.execute(
            "create table if not exists log("
            "id integer primary key autoincrement,"
            "time datetime default(datetime('now')) not null,"
            "package integer not null,"
            "key integer not null,"
            "value text,"
            "foreign key(key) references keys(id))"
        )
        self.execute("create index if not exists log_time on log(time)")
        self.execute("create index if not exists log_package on log(package)")
        self.execute("create index if not exists log_value on log(value)")
        self.execute("create index if not exists log_id_key on log(id,key)")

    def insert(self, packages: typing.Iterable[tuple[int | None, dict[str, str]]]):
        buf = []
        for p in filter(lambda p: p[1], packages):
            if p[0] is not None:
                buf.extend(f"({p[0]},{self.key_id(k)},'{v}')" for k, v in p[1].items())
            else:
                rows = [(p[0], k, str(v)) for k, v in p[1].items()]
                p_id = self.execute(
                    f"insert into log(package,key,value)values"
                    f"(coalesce((select max(package)from log),-1)+1,{self.key_id(rows[0][1])},'{rows[0][2]}')"
                    "returning package"
                ).__next__()[0]
                buf.extend(f"({p_id},{self.key_id(r[1])},'{r[2]}')" for r in rows[1:])
        self.execute(f"insert into log(package,key,value)values" + ",".join(buf))

    @functools.cache
    def key_id(self, key: str) -> int:
        if result := list(self.execute_enum(f"select id from keys where key='{key}'")):
            return result[0][0]
        return next(
            self.execute_enum(f"insert into keys(key)values('{key}')returning *")
        )[0]

    @functools.cache
    def id_key(self, id: int) -> str:
        return next(self.execute_enum(f"select key from keys where id={id}"))[0]

    def select(
        self,
        present: dict[str, str | typing.Literal[True]],
        absent: dict[str, str | typing.Literal[True]] = {},
        get: set[str] = set(),
    ):
        query = "select package,key,value from log where " + " and ".join(
            f"package {clause} (select package from log where key={self.key_id(key)}"
            + (f" and value='{value}')" if value != True else ")")
            for clause, d in (("in", present), ("not in", absent))
            for key, value in d.items()
        )
        if get:
            query += "and(" + " or ".join(f"key={self.key_id(k)}" for k in get) + ")"
        current = {}
        for row in self.execute(query + "order by package,id"):
            if "package" in current and current["package"] != row[0]:
                yield current
                current = {}
            current |= {"package": row[0], self.id_key(row[1]): row[2]}
        yield current
