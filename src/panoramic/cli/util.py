import itertools


def peek_iterator(gen):
    """
    Peek first item in iterator
    """
    peek = next(gen)
    return peek, itertools.chain([peek], gen)
