import datetime

import pandas as pd
import polars as pl
import pytest

from src.solution_pandas import split_based_on_amount as split_based_on_amount_pandas
from src.solution_polars import split_based_on_amount as split_based_on_amount_polars


@pytest.mark.parametrize("solution", ["polars", "pandas"])
def test_split_based_on_amt_single_pair(solution: str) -> None:
    """test_split_based_on_amt_single_pair
    Verify if splitting tranactions based on amount works for a  single account_id - counterparty_id pair.
    We construct a set of 5 transactions, all belonging to the same account_id - counterparty_id pair.
    We choose the amounts to be [3.99, 50.00, 3.99, 59.99, 3.99] (example from the blog).
    We expect the amounts to be nicely split in two groups, a group with values above and below 7.98 (2 * median (i.e., 3.99) = 7.98)
    """
    booking_amount = [3.99, 50.00, 3.99, 59.99, 3.99]
    data = {
        "date": [datetime.date(2025, 1, 1)] * 5,
        "account_id": ["acct_id"] * 5,
        "counterparty_id": ["ctpty_acct_id"] * 5,
        "booking_amount": booking_amount,
    }
    if solution == "polars":
        df = pl.DataFrame(data)
        df_res = split_based_on_amount_polars(df.lazy()).collect()
    if solution == "pandas":
        df = pd.DataFrame(data)
        df_res = split_based_on_amount_pandas(df)

    assert df_res["sequence_id"].to_list() == [0, 1, 0, 1, 0]


@pytest.mark.parametrize("solution", ["polars", "pandas"])
def test_split_based_on_amt_multiple_pairs(solution: str) -> None:
    """test_split_based_on_amt_multiple_pairs
    Verify if splitting tranactions based on amount works for multiple account_id - counterparty_id pair.

    We construct 2 pairs:
    pair 1:
    We construct a set of 5 transactions, all belonging to the same account_id - counterparty_id pair.
    We choose the amounts to be  [3.99, 50.00, 3.99, 59.99, 3.99].
    We expect the amounts to be nicely split in two groups, a group with values above and below 7.98 (2 * median (i.e., 3.99) = 7.98)
    pair 2:
    We construct a set of 5 transactions, all belonging to the same account_id - counterparty_id pair.
    We choose the amounts to be [10, 20, 30, 40, 50].
    We expect all transactions in a single group since all  amounts  are larger  than 60, i.e., 2 * median (30) of all transactions.
    """

    # pair  1:
    booking_amount_1 = [3.99, 50.00, 3.99, 59.99, 3.99]
    data_1 = {
        "date": [datetime.date(2025, 1, 1)] * 5,
        "account_id": ["acct_id_1"] * 5,
        "counterparty_id": ["ctpty_acct_id_1"] * 5,
        "booking_amount": booking_amount_1,
    }
    # pair  2:
    booking_amount_2 = [10.0, 20.0, 30.0, 40.0, 50.0]
    data_2 = {
        "date": [datetime.date(2025, 1, 1)] * 5,
        "account_id": ["acct_id_2"] * 5,
        "counterparty_id": ["ctpty_acct_id_2"] * 5,
        "booking_amount": booking_amount_2,
    }

    if solution == "polars":
        df_1 = pl.DataFrame(data_1)
        df_2 = pl.DataFrame(data_2)
        df = pl.concat([df_1, df_2])
        df_res = split_based_on_amount_polars(df.lazy()).collect()
    if solution == "pandas":
        df_1 = pd.DataFrame(data_1)
        df_2 = pd.DataFrame(data_2)
        df = pd.concat([df_1, df_2])
        df_res = split_based_on_amount_pandas(df)

    assert df_res["sequence_id"].to_list() == [0, 1, 0, 1, 0, 0, 0, 0, 0, 0]
