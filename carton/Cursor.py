import abc
import typing


class Cursor(abc.ABC):
    @abc.abstractmethod
    def execute(self, query: str, arguments: typing.Tuple[typing.Any, ...] = ()) -> typing.Any:
        ...

    @abc.abstractmethod
    def executemany(
        self, query: str, arguments: typing.Union[typing.List[typing.Tuple[typing.Any, ...]], None] = None
    ) -> typing.Any:
        ...
