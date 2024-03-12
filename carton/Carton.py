import dataclasses
import typing

from .Database import Database
from .Subject import Subject


@dataclasses.dataclass(frozen=True)
class Carton:
    db: Database
    cache: typing.Dict[str, int] = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        self.db.create()

    def predicate(self, key: str, value: typing.Union[str, None]):
        return f"{key}={value or ''}"

    def key_value(self, predicate: str):
        splitter = predicate.find("=")
        return predicate[:splitter], predicate[splitter + 1 :] or None

    def insert(self, subjects: typing.Iterable[Subject]):
        cursor = self.db.cursor()
        insert_buf = []
        update_buf = []
        for s in subjects:
            update_buf.extend((r[0],) for r in s.update.values())
            create = list(s.create.items())
            subject_id = s.id
            if subject_id is None:
                subject_id = cursor.execute(
                    "insert into sentences(predicate) values(?) returning subject", (self.predicate_id(*create.pop(0)),)
                ).__next__()[0]
            insert_buf.extend((subject_id, self.predicate_id(e[0], e[1])) for e in create)
            insert_buf.extend((subject_id, self.predicate_id(k, r[1])) for k, r in s.update.items())
        cursor.executemany("update sentences set actual=false where id=?", update_buf)
        cursor.executemany("insert into sentences(subject, predicate) values(?,?)", insert_buf)
        self.db.commit()

    def predicate_id(self, key: str, value: typing.Union[str, None]) -> int:
        p = self.predicate(key, value)
        try:
            return next(self.db.cursor().execute("select id from predicates where predicate=?", (p,)))[0]
        except StopIteration:
            result = next(self.db.cursor().execute("insert into predicates(predicate) values(?) returning id", (p,)))[0]
            self.db.commit()
            return result

    def select(self, key: str, value: typing.Union[str, None], *, cache: bool = False):
        if cache:
            predicate = self.predicate(key, value)
            if predicate not in self.cache:
                self.cache[predicate] = self.predicate_id(key, value)
            subjects = self.db.cursor().execute(
                "select subject from sentences where predicate=? and actual=true", (self.cache[predicate],)
            )
        else:
            subjects = self.db.cursor().execute(
                "select subject from sentences as s join predicates as p "
                "on s.predicate=p.id and s.actual=true and p.predicate=?",
                (self.predicate(key, value),),
            )
        for (subject,) in subjects:
            d = {}
            for sentence_id, predicate in self.db.cursor().execute(
                "select s.id, p.predicate from sentences as s join predicates as p "
                "on s.predicate=p.id and s.actual=true and s.subject=?",
                (subject,),
            ):
                key, value = self.key_value(predicate)
                d[key] = (sentence_id, value)
            yield Subject(subject, current=d)
