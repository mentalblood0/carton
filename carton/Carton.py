import dataclasses
import operator
import typing


@dataclasses.dataclass(frozen=True)
class Carton:
    execute: typing.Callable[[], typing.Callable[[str, typing.Tuple[typing.Any, ...]], typing.Any]]
    executemany: typing.Callable[[], typing.Callable[[str, typing.List[typing.Tuple[typing.Any, ...]]], typing.Any]]
    integer: str = "integer"
    now: str = "datetime('now')"
    key_id_cache: typing.Dict[str, int] = dataclasses.field(default_factory=dict)
    id_key_cache: typing.Dict[int, str] = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        execute = self.execute()
        execute("create table if not exists keys(id integer primary key autoincrement,key text unique)", ())
        execute("create index if not exists keys_key on keys(key)", ())
        execute(
            "create table if not exists carton("
            f"id {self.integer} primary key autoincrement,"
            f"time datetime default({self.now}) not null,"
            f"package {self.integer} not null,"
            "key integer not null,"
            "value text,"
            "foreign key(key) references keys(id))",
            (),
        )
        execute("create index if not exists carton_time on carton(time)", ())
        execute("create index if not exists carton_package on carton(package)", ())
        execute("create index if not exists carton_value on carton(value)", ())
        execute("create index if not exists carton_id_key on carton(id,key)", ())
        execute("create index if not exists carton_package_key_value on carton(package,key,value)", ())

    def insert(self, packages: typing.Iterable[typing.Tuple[typing.Union[int, None], typing.Dict[str, str]]]):
        execute = self.execute()
        buf = []
        for p in filter(operator.itemgetter(1), packages):
            if p[0] is not None:
                buf.extend((p[0], self.key_id(k), v) for k, v in p[1].items())
            else:
                k_v = list(p[1].items())
                p_id = execute(
                    "insert into carton(package,key,value)values(coalesce((select max(package)from carton),-1)+1,?,?)"
                    "returning package",
                    (self.key_id(k_v[0][0]), k_v[0][1]),
                ).__next__()[0]
                buf.extend((p_id, self.key_id(e[0]), e[1]) for e in k_v[1:])
        self.executemany()("insert into carton(package,key,value)values(?,?,?)", buf)

    def key_id(self, key: str) -> int:
        if key not in self.key_id_cache:
            try:
                self.key_id_cache[key] = next(self.execute()("select id from keys where key=?", (key,)))[0]
            except StopIteration:
                self.key_id_cache[key] = next(self.execute()("insert into keys(key)values(?)returning *", (key,)))[0]
        return self.key_id_cache[key]

    def id_key(self, i: int) -> str:
        if i not in self.id_key_cache:
            self.id_key_cache[i] = next(self.execute()("select key from keys where id=?", (i,)))[0]
        return self.id_key_cache[i]

    def select(
        self,
        present: typing.Union[typing.Dict[str, typing.Union[str, bool]], None] = None,
        absent: typing.Union[typing.Dict[str, typing.Union[str, bool]], None] = None,
        get: typing.Union[typing.Set[str], None] = None,
        exclude: typing.Union[typing.Set[int], None] = None,
    ):
        query = "select c.package,c.key,c.value from (select package,key,value from carton"
        if exclude:
            query += " where " + " or ".join(f"package!={e}" for e in exclude)
        if get:
            query += " where " + " or ".join(f"key={self.key_id(k)}" for k in get)
        query += " order by package) as c"
        for i, (k, v) in enumerate((present or {}).items()):
            query += f" join carton as c{i} on c.package=c{i}.package and c{i}.key={self.key_id(k)}"
            if v is not True:
                query += f" and c{i}.value='{v}'"
        for i, (k, v) in enumerate((absent or {}).items()):
            query += f" join carton as c{i} on c.package=c{i}.package and c{i}.key!={self.key_id(k)}"
            if v is not True:
                query += f" and c{i}.value='{v}'"
        current = {}
        for row in self.execute()(query, ()):
            if "package" in current and current["package"] != row[0]:
                yield current
                current = {}
            current.update({"package": row[0], self.id_key(row[1]): row[2]})
        if "package" in current:
            yield current
