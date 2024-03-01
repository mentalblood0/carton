import dataclasses
import operator
import typing

from .Database import Database


@dataclasses.dataclass(frozen=True)
class Carton:
    db: Database
    key_id_cache: typing.Dict[str, int] = dataclasses.field(default_factory=dict)
    id_key_cache: typing.Dict[int, str] = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        self.db.create()

    def insert(
        self,
        packages: typing.Iterable[typing.Tuple[typing.Union[int, None], typing.Dict[str, typing.Union[str, None]]]],
    ):
        cursor = self.db.cursor()
        update_buf = []
        insert_buf = []
        for p in filter(operator.itemgetter(1), packages):
            if p[0] is not None:
                update_buf.extend((p[0], self.key_id(k)) for k in p[1].keys())
                insert_buf.extend((p[0], self.key_id(k), v) for k, v in p[1].items())
            else:
                k_v = list(p[1].items())
                p_id = cursor.execute(
                    "insert into carton(package,key,value)"
                    "values(coalesce((select max(package)from carton),-1)+1,?,?)"
                    "returning package",
                    (self.key_id(k_v[0][0]), k_v[0][1]),
                ).__next__()[0]
                insert_buf.extend((p_id, self.key_id(e[0]), e[1]) for e in k_v[1:])
        cursor.executemany("update carton set actual=false where package=? and key=? and actual=true", update_buf)
        cursor.executemany("insert into carton(package,key,value)values(?,?,?)", insert_buf)
        self.db.commit()

    def key_id(self, key: str) -> int:
        if key not in self.key_id_cache:
            try:
                self.key_id_cache[key] = next(self.db.cursor().execute("select id from keys where key=?", (key,)))[0]
            except StopIteration:
                self.key_id_cache[key] = next(
                    self.db.cursor().execute("insert into keys(key)values(?)returning *", (key,))
                )[0]
        return self.key_id_cache[key]

    def id_key(self, i: int) -> str:
        if i not in self.id_key_cache:
            self.id_key_cache[i] = next(self.db.cursor().execute("select key from keys where id=?", (i,)))[0]
        return self.id_key_cache[i]

    def select(
        self,
        present: typing.Union[typing.Dict[str, typing.Union[str, bool, None]], None] = None,
        get: typing.Union[typing.Set[str], None] = None,
        exclude: typing.Union[typing.Set[int], None] = None,
    ):
        query = "select c.package,c.key,c.value from (select package,key,value from carton where actual=true"
        if exclude:
            query += f" and package not in ({','.join(str(e) for e in exclude)})"
        if get:
            query += f" and key in ({','.join(str(self.key_id(k)) for k in get)})"
        query += " order by package) as c"

        for c, (k, v) in enumerate(sorted((present or {}).items(), key=lambda p: "a" if p[1] is None else "b")):
            query += (
                f" join carton as c{c} on c.package=c{c}.package"
                f" and c{c}.actual=true and c{c}.key={self.key_id(k)} and c{c}.value"
            )
            if v is None:
                query += " is null"
            elif v is True:
                query += " is not null"
            else:
                query += f"='{v}'"

        current = {}
        for row in self.db.cursor().execute(query, ()):
            if "package" in current and current["package"] != row[0]:
                yield current
                current = {}
            current.update({"package": row[0], self.id_key(row[1]): row[2]})
        if "package" in current:
            yield current
