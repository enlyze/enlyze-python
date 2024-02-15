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


class TestTimeseriesData:
    def test_merge(self, timeseries_data_1, timeseries_data_2):
        timeseries_data_1_records_len = len(timeseries_data_1.records)
        timeseries_data_1_columns_len = len(timeseries_data_1.columns)
        timeseries_data_2_records_len = len(timeseries_data_2.records)
        timeseries_data_2_columns_len = len(timeseries_data_2.columns)
        expected_merged_record_len = len(timeseries_data_1.records[0]) + len(
            timeseries_data_2.records[0][TIMESTAMP_OFFSET:]
        )

        merged = timeseries_data_1.merge(timeseries_data_2)

        assert merged is timeseries_data_1
        assert (
            len(merged.records)
            == timeseries_data_1_records_len
            == timeseries_data_2_records_len
        )
        assert (
            len(merged.columns)
            == timeseries_data_1_columns_len
            + timeseries_data_2_columns_len
            - TIMESTAMP_OFFSET
        )

        for r in merged.records:
            assert len(r) == expected_merged_record_len

    def test_merge_raises_number_of_records_mismatch(
        self, timeseries_data_1, timeseries_data_2
    ):
        timeseries_data_2.records = timeseries_data_2.records[1:]
        with pytest.raises(
            ValueError, match="Number of records in both instances has to be the same"
        ):
            timeseries_data_1.merge(timeseries_data_2)

    def test_merge_raises_mismatched_timestamps(
        self, timeseries_data_1, timeseries_data_2, timestamp
    ):
        timeseries_data_2.records[0][0] = (timestamp - timedelta(days=1)).isoformat()

        with pytest.raises(ValueError, match="mismatched timestamps"):
            timeseries_data_1.merge(timeseries_data_2)
