from typing import Iterable, Any, Tuple
import itertools


def peek_iterator(gen: Iterable[Any]) -> Tuple[Any, Iterable]:
    """
    Peek first item in iterator
    """
    peek = next(gen)
    return peek, itertools.chain([peek], gen)
