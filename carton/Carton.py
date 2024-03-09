import dataclasses
import itertools
import operator
import typing

from .Database import Database


@dataclasses.dataclass(frozen=True)
class Carton:
    db: Database

    def __post_init__(self):
        self.db.create()

    def predicate(self, key: str, value: typing.Union[str, None]):
        return f"{key}={value or ''}"

    def key_value(self, predicate: str):
        splitter = predicate.find("=")
        return predicate[:splitter], predicate[splitter + 1 :] or None

    def insert(
        self,
        sentences: typing.Iterable[typing.Tuple[typing.Union[int, None], typing.Dict[str, typing.Union[str, None]]]],
    ):
        cursor = self.db.cursor()
        insert_buf = []
        update_buf = []
        for p in filter(operator.itemgetter(1), sentences):
            if p[0] is not None:
                update_buf.extend((p[0], f"{self.predicate(k, None)}%") for k in p[1])
                insert_buf.extend((p[0], self.predicate_id(self.predicate(k, v))) for k, v in p[1].items())
            else:
                k_v = list(p[1].items())
                p_id = cursor.execute(
                    "insert into sentences(subject,predicate)"
                    "values(coalesce((select max(subject)from sentences),-1)+1,?)returning subject",
                    (self.predicate_id(self.predicate(k_v[0][0], k_v[0][1])),),
                ).__next__()[0]
                insert_buf.extend((p_id, self.predicate_id(self.predicate(e[0], e[1]))) for e in k_v[1:])
        cursor.executemany(
            "update sentences set actual=false where subject=? "
            "and predicate in (select id from predicates where predicate like ? limit 1) and actual=true",
            update_buf,
        )
        cursor.executemany("insert into sentences(subject,predicate)values(?,?)", insert_buf)
        self.db.commit()

    def predicate_id(self, key: str, *, create: bool = True) -> int:
        try:
            return next(self.db.cursor().execute("select id from predicates where predicate=?", (key,)))[0]
        except StopIteration:
            if create:
                result = next(
                    self.db.cursor().execute("insert into predicates(predicate)values(?)returning *", (key,))
                )[0]
                self.db.commit()
                return result
            raise

    def select(self, key: str, value: typing.Union[str, None]):
        for (subject,) in self.db.cursor().execute(
            "select subject from sentences where actual=true and predicate=?",
            (self.predicate_id(self.predicate(key, value), create=False),),
        ):
            yield dict(
                list(
                    itertools.starmap(
                        self.key_value,
                        self.db.cursor().execute(
                            "select p.predicate from sentences as s join predicates as p "
                            "on s.predicate=p.id and s.actual=true and s.subject=?",
                            (subject,),
                        ),
                    )
                )
                + [("subject", subject)]
            )
