import pathlib
import pickle
import typing as t

import pytest

from stream_framework.serializers.activity_serializer import pickle_dump, pickle_load


@pytest.mark.parametrize(
    "data",
    [
        {},
        {"foo": "bar"},
        {"🙂": "🙃"},
        {"path": pathlib.Path(__file__)},
    ],
)
def test_pickle_dump_and_load(data: dict[str, t.Any]):
    """Make sure data can be pickled and restored."""

    dump_str = pickle_dump(data)
    got = pickle_load(dump_str)
    assert got == data


@pytest.mark.parametrize(
    "data",
    [
        {"foo": "bar"},
        {"🙂": "🙃"},
        {"path": pathlib.Path(__file__)},
    ],
)
def test_pickle_load_compatibility(data: dict[str, t.Any]):
    """Make sure backwards compatibile loading works properly."""

    dump_str = pickle.dumps(data, protocol=3).decode("latin1")
    got = pickle_load(dump_str)
    assert got == data
