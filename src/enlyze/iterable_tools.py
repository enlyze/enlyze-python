from typing import Iterable, Sequence, TypeVar

MINIMUM_CHUNK_SIZE = 1

T = TypeVar("T")


def chunk(seq: Sequence[T], chunk_size: int) -> Iterable[Sequence[T]]:
    if chunk_size < MINIMUM_CHUNK_SIZE:
        raise ValueError(f"{chunk_size=} is less than {MINIMUM_CHUNK_SIZE=}")

    return (seq[i : i + chunk_size] for i in range(0, len(seq), chunk_size))
