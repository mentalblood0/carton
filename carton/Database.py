import abc

from .Cursor import Cursor


class Database(abc.ABC):
    @abc.abstractmethod
    def create(self) -> None:
        ...

    @abc.abstractmethod
    def cursor(self) -> Cursor:
        ...

    @abc.abstractmethod
    def commit(self) -> None:
        ...
