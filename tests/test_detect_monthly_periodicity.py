import datetime

import pandas as pd
import polars as pl
import pytest

from src.solution_pandas import (
    detect_monthly_periodicity as detect_monthly_periodicity_pandas,
)
from src.solution_polars import (
    detect_monthly_periodicity as detect_monthly_periodicity_polars,
)


@pytest.mark.parametrize(
    "dates, monthly_consistency_score",
    # case 1: perfect monthly pattern of 5 transactions -> monthly_consistency_score = 1
    # case 2: monthly pattern of 5 transaction, but with a 2 month gap between the last  2 transactions -> monthly_consistency_score = 0.75 (3/4 time  deltas)
    [
        (
            [
                datetime.date(2025, 1, 1),
                datetime.date(2025, 2, 1),
                datetime.date(2025, 3, 1),
                datetime.date(2025, 4, 1),
                datetime.date(2025, 5, 1),
            ],
            1.0,
        ),
        (
            [
                datetime.date(2025, 1, 1),
                datetime.date(2025, 2, 1),
                datetime.date(2025, 3, 1),
                datetime.date(2025, 4, 1),
                datetime.date(2025, 6, 1),  # NOTE: 2 month gap
            ],
            0.75,
        ),
    ],
)
@pytest.mark.parametrize("solution", ["polars", "pandas"])
def test_detect_monthly_periodicity_single_pair(
    solution: str, dates: list[datetime.date], monthly_consistency_score: float
) -> None:
    """test_detect_monthly_periodicity
    Verify if we can detect the possibiility of a monthly sequence
    """

    data = {
        "date": dates,
        "account_id": ["acct_id"] * 5,
        "counterparty_id": ["ctpty_acct_id"] * 5,
        "booking_amount": [42] * 5,
        "sequence_id": [1] * 5,
    }
    if solution == "polars":
        df = pl.DataFrame(data)
        df_res = detect_monthly_periodicity_polars(df.lazy()).collect()
        assert df_res.select("monthly_consistency_score").item(0, 0) == monthly_consistency_score

    if solution == "pandas":
        df = pd.DataFrame(data)
        df_res = detect_monthly_periodicity_pandas(df)
        assert df_res["monthly_consistency_score"][0] == monthly_consistency_score


@pytest.mark.parametrize("solution", ["polars", "pandas"])
def test_detect_monthly_periodicity_multiple_pairs(solution: str) -> None:
    """test_detect_monthly_periodicity
    Verify if we can detect the possibiility of a monthly sequence
    """

    dates_1 = [
        datetime.date(2025, 1, 1),
        datetime.date(2025, 2, 1),
        datetime.date(2025, 3, 1),
        datetime.date(2025, 4, 1),
        datetime.date(2025, 5, 1),
    ]

    data_1 = {
        "date": dates_1,
        "account_id": ["acct_id_1"] * 5,
        "counterparty_id": ["ctpty_acct_id_1"] * 5,
        "booking_amount": [42] * 5,
        "sequence_id": [1] * 5,
    }

    dates_2 = [
        datetime.date(2025, 1, 1),
        datetime.date(2025, 2, 1),
        datetime.date(2025, 3, 1),
        datetime.date(2025, 4, 1),
        datetime.date(2025, 6, 1),  # NOTE: 2 month gap
    ]

    data_2 = {
        "date": dates_2,
        "account_id": ["acct_id_2"] * 5,
        "counterparty_id": ["ctpty_acct_id_2"] * 5,
        "booking_amount": [42] * 5,
        "sequence_id": [1] * 5,
    }

    if solution == "polars":
        df_1 = pl.DataFrame(data_1)
        df_2 = pl.DataFrame(data_2)
        df = pl.concat([df_1, df_2])
        df_res = detect_monthly_periodicity_polars(df.lazy()).collect()
        assert df_res.filter(pl.col("account_id") == "acct_id_1").select("monthly_consistency_score").item(0, 0) == 1
        assert (
            df_res.filter(pl.col("account_id") == "acct_id_2").select("monthly_consistency_score").item(0, 0) == 0.75
        )
    if solution == "pandas":
        df_1 = pd.DataFrame(data_1)
        df_2 = pd.DataFrame(data_2)
        df = pd.concat([df_1, df_2])
        df_res = detect_monthly_periodicity_pandas(df)
        assert df_res.query("account_id == 'acct_id_1'")["monthly_consistency_score"][0] == 1
        assert df_res.query("account_id == 'acct_id_2'")["monthly_consistency_score"][0] == 0.75

    print(df_res)
