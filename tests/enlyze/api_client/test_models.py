import itertools
import random
from datetime import datetime, timedelta, timezone

import pytest

from enlyze.api_client.models import TimeseriesData

# We use this to skip columns that contain the timestamp assuming
# it starts at the beginning of the sequence. We also use it
# when computing lengths to account for a timestamp column.
TIMESTAMP_OFFSET = 1
NOW = datetime.now(tz=timezone.utc)


def _generate_timeseries_data(*, columns, number_of_records):
    timeseries_columns = ["time"]
    timeseries_columns.extend(columns)

    counter = itertools.count(start=10)

    return TimeseriesData(
        columns=timeseries_columns,
        records=[
            [
                (NOW - timedelta(minutes=next(counter))).isoformat(),
                *[random.randint(1, 100) for _ in range(len(columns))],
            ]
            for _ in range(number_of_records)
        ],
    )


class TestTimeseriesData:
    @pytest.mark.parametrize(
        "data_parameters,data_to_merge_parameters",
        [
            (
                {"columns": ["var1", "var2"], "number_of_records": 1},
                {"columns": ["var3"], "number_of_records": 1},
            ),
            (
                {"columns": ["var1", "var2"], "number_of_records": 1},
                {"columns": ["var3"], "number_of_records": 3},
            ),
        ],
    )
    def test_merge(self, data_parameters, data_to_merge_parameters):
        data = _generate_timeseries_data(**data_parameters)
        data_to_merge = _generate_timeseries_data(**data_to_merge_parameters)
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

    @pytest.mark.parametrize(
        "data_parameters,data_to_merge_parameters",
        [
            (
                {"columns": ["var1", "var2"], "number_of_records": 2},
                {"columns": ["var3"], "number_of_records": 1},
            ),
        ],
    )
    def test_merge_raises_number_of_records_to_merge_less_than_existing(
        self, data_parameters, data_to_merge_parameters
    ):
        data = _generate_timeseries_data(**data_parameters)
        data_to_merge = _generate_timeseries_data(**data_to_merge_parameters)

        with pytest.raises(
            ValueError,
            match=(
                "The instance to merge must have a number of"
                " records greater than or equal to the number"
                " of records of the instance you're trying to merge into."
            ),
        ):
            data.merge(data_to_merge)

    @pytest.mark.parametrize(
        "data_parameters,data_to_merge_parameters",
        [
            (
                {"columns": ["var1", "var2"], "number_of_records": 1},
                {"columns": ["var3"], "number_of_records": 1},
            ),
        ],
    )
    def test_merge_raises_mismatched_timestamps(
        self, data_parameters, data_to_merge_parameters
    ):
        data = _generate_timeseries_data(**data_parameters)
        data_to_merge = _generate_timeseries_data(**data_to_merge_parameters)

        data_to_merge.records[0][0] = (NOW - timedelta(days=1)).isoformat()

        with pytest.raises(ValueError, match="mismatched timestamps"):
            data.merge(data_to_merge)
