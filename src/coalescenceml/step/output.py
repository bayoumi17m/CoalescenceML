from typing import Any, Iterator, NamedTuple, Tuple, Type


class Output:
    """Special object to store namedtuple with immutable name."""

    def __init__(self, **kwargs: Type[Any]):
        self.outputs = NamedTuple("CoalescenceOutput", **kwargs)

    def items(self) -> Iterator[Tuple[str, Type[Any]]]:
        """Items returns an iterator over the output.

        Yields:
            Tuple[str, Type[Any]]: A pair of the output name and type
        """
        yield from self.outputs._asdict().items()
