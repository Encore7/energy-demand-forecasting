from __future__ import annotations

import os

import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.fs as pafs
from pyarrow.fs import FileSelector

# pylint: disable=no-member

REQUIRED = (
    "dt",
    "day_of_week",
    "day_of_month",
    "week_of_year",
    "month",
    "year",
    "is_weekend",
    "is_holiday",
    "is_working_day",
    "season",
)


def main() -> None:
    run_date = os.environ["RUN_DATE"]
    repo = os.environ["LAKEFS_REPO"]
    branch = os.environ["LAKEFS_BRANCH"]

    fs = pafs.S3FileSystem(
        access_key=os.environ["LAKEFS_ACCESS_KEY_ID"],
        secret_key=os.environ["LAKEFS_SECRET_ACCESS_KEY"],
        endpoint_override=os.environ["LAKEFS_S3_ENDPOINT_INTERNAL"],
        scheme="http",
    )

    silver_path = f"{repo}/{branch}/silver/calendar/dt={run_date}/"

    selector = FileSelector(silver_path, recursive=False)
    infos = fs.get_file_info(selector)
    if not infos:
        raise FileNotFoundError(f"No silver calendar files under {silver_path}")

    dataset = ds.dataset(silver_path, filesystem=fs, format="parquet")
    table = dataset.to_table(columns=list(REQUIRED))

    rows = table.num_rows
    assert rows == 1, f"Expected 1 calendar row, got {rows}"

    # Basic null checks
    for col in REQUIRED:
        assert table.column(col).null_count == 0, f"{col} has nulls"

    # dt matches RUN_DATE
    dt_str = pc.cast(table.column("dt"), "string")
    bad_dt = pc.sum(pc.not_equal(dt_str, run_date)).as_py()
    assert bad_dt == 0, f"Found {bad_dt} rows where dt != {run_date}"

    print(f"PASS: silver calendar dt={run_date} rows={rows}")


if __name__ == "__main__":
    main()
