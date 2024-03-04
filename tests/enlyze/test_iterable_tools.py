from typing import Sequence

import pytest
from hypothesis import given
from hypothesis.strategies import integers, lists

from enlyze.iterable_tools import MINIMUM_CHUNK_SIZE, chunk


@given(
    seq=lists(integers()),
    chunk_size=integers(min_value=MINIMUM_CHUNK_SIZE),
)
def test_chunk(seq: Sequence[int], chunk_size: int):
    result = list(chunk(seq, chunk_size))
    assert sum(len(sublist) for sublist in result) == len(seq)
    assert all(len(sublist) <= chunk_size for sublist in result)


@given(
    seq=lists(integers()),
    chunk_size=integers(max_value=MINIMUM_CHUNK_SIZE - 1),
)
def test_chunk_raises_invalid_chunk_size(seq: Sequence[int], chunk_size: int):
    with pytest.raises(ValueError):
        chunk(seq, chunk_size)
