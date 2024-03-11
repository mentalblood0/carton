import dataclasses
import typing


@dataclasses.dataclass(frozen=True)
class Subject:
    id: typing.Union[int, None] = None
    create: typing.Dict[str, typing.Union[str, None]] = dataclasses.field(default_factory=dict)
    current: typing.Dict[str, typing.Tuple[int, typing.Union[str, None]]] = dataclasses.field(default_factory=dict)
    update: typing.Dict[str, typing.Tuple[typing.Union[int, None], typing.Union[str, None]]] = dataclasses.field(
        default_factory=dict
    )

    def __getitem__(self, key: str):
        if key in self.update:
            return self.update[key][-1]
        if key in self.create:
            return self.create[key]
        return self.current[key][-1]

    def __setitem__(self, key: str, value: typing.Union[str, None]):
        if key in self.current:
            self.update[key] = (self.current[key][0], value)
        else:
            self.create[key] = value
