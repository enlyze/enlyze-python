from datetime import datetime, timedelta, timezone

import pytest

from enlyze.api_clients.timeseries.models import TimeseriesData

# We use this to skip  columns that contain the timestamp assuming
# it starts at the beginning of the sequence. We also use it
# when computing lengths to account for a timestamp column.
TIMESTAMP_OFFSET = 1


@pytest.fixture
def timestamp():
    return datetime.now(tz=timezone.utc)


@pytest.fixture
def timeseries_data_1(timestamp):
    return TimeseriesData(
        columns=["time", "var1", "var2"],
        records=[
            [timestamp.isoformat(), 1, 2],
            [(timestamp - timedelta(minutes=10)).isoformat(), 3, 4],
        ],
    )


@pytest.fixture
def timeseries_data_2(timestamp):
    return TimeseriesData(
        columns=["time", "var3"],
        records=[
            [timestamp.isoformat(), 5],
            [(timestamp - timedelta(minutes=10)).isoformat(), 6],
        ],
    )


@pytest.fixture
def timeseries_data_3(timestamp):
    return TimeseriesData(
        columns=["time", "var3"],
        records=[
            [timestamp.isoformat(), 5],
            [(timestamp - timedelta(minutes=10)).isoformat(), 6],
            [(timestamp - timedelta(minutes=10)).isoformat(), 7],
            [(timestamp - timedelta(minutes=10)).isoformat(), 8],
        ],
    )


class TestTimeseriesData:
    @pytest.mark.parametrize(
        "data_fixture,data_to_merge_fixture",
        [
            ("timeseries_data_1", "timeseries_data_2"),
            ("timeseries_data_1", "timeseries_data_3"),
        ],
    )
    def test_merge(self, request, data_fixture, data_to_merge_fixture):
        data = request.getfixturevalue(data_fixture)
        data_to_merge = request.getfixturevalue(data_to_merge_fixture)
        data_records_len = len(data.records)
        data_columns_len = len(data.columns)
        data_to_merge_columns_len = len(data_to_merge.columns)
        expected_merged_record_len = len(data.records[0]) + len(
            data_to_merge.records[0][TIMESTAMP_OFFSET:]
        )

        merged = data.merge(data_to_merge)

        assert merged is data
        assert len(merged.records) == data_records_len
        assert (
            len(merged.columns)
            == data_columns_len + data_to_merge_columns_len - TIMESTAMP_OFFSET
        )

        for r in merged.records:
            assert len(r) == expected_merged_record_len == len(merged.columns)

    def test_merge_raises_number_of_records_to_merge_less_than_existing(
        self, timeseries_data_1, timeseries_data_2
    ):
        timeseries_data_2.records = timeseries_data_2.records[1:]
        with pytest.raises(
            ValueError,
            match=(
                "The instance to merge must have a number of"
                " records greater than or equal to the number"
                " of records of the instance you're trying to merge into."
            ),
        ):
            timeseries_data_1.merge(timeseries_data_2)

    def test_merge_raises_mismatched_timestamps(
        self, timeseries_data_1, timeseries_data_2, timestamp
    ):
        timeseries_data_2.records[0][0] = (timestamp - timedelta(days=1)).isoformat()

        with pytest.raises(ValueError, match="mismatched timestamps"):
            timeseries_data_1.merge(timeseries_data_2)
