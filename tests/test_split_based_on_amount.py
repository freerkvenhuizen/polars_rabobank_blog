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
    We choose the amounts to be [1, 4, 4, 4, 4].
    We expect the first transaction to be split of since it is smaller than  0.3 * median of all transactions.
    0.3 * median (i.e., 4) = 1.2
    # the first element is smaller then 1.2, the rest larger
    """
    booking_amount = [1, 4, 4, 4, 4]
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

    assert df_res["sequence_id"].to_list() == [0, 1, 1, 1, 1]


@pytest.mark.parametrize("solution", ["polars", "pandas"])
def test_split_based_on_amt_multiple_pairs(solution: str) -> None:
    """test_split_based_on_amt_multiple_pairs
    Verify if splitting tranactions based on amount works for multiple account_id - counterparty_id pair.

    We construct 2 pairs:
    pair 1:
    We construct a set of 5 transactions, all belonging to the same account_id - counterparty_id pair.
    We choose the amounts to be [1, 4, 4, 4, 4].
    We expect the first transaction to be split of since it is smaller than  0.3 * median of all transactions.
    0.3 * median (i.e., 4) = 1.2
    # the first element is smaller then 1.2, the rest larger
    pair 2:
    We construct a set of 5 transactions, all belonging to the same account_id - counterparty_id pair.
    We choose the amounts to be [10, 20, 30, 40, 50].
    We expect all transactions in a single all all  amount  aree larger  than  0.3 * median of all transactions.
    0.3 * median (i.e., 30) = 9
    """

    # pair  1:
    booking_amount_1 = [1, 4, 4, 4, 4]
    data_1 = {
        "date": [datetime.date(2025, 1, 1)] * 5,
        "account_id": ["acct_id_1"] * 5,
        "counterparty_id": ["ctpty_acct_id_1"] * 5,
        "booking_amount": booking_amount_1,
    }
    # pair  2:
    booking_amount_2 = [10, 20, 30, 40, 50]
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

    assert df_res["sequence_id"].to_list() == [0, 1, 1, 1, 1, 0, 0, 0, 0, 0]
