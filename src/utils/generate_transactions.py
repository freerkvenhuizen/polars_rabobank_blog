import datetime
import random
import uuid
from datetime import timedelta
from math import floor, sqrt

import polars as pl


def generate_random_transactions(n_trx: int, avg_n_trx_per_pair: int) -> pl.DataFrame:
    """generate_random_transactions Generate 'n_trx' random recurring transactions between account_id and counterparty_id pairs.
    This function randomly generates transactions:
    - the number of pairs is random, but should be close to 'n_trx' / 'avg_n_trx_per_pair'
    - The number of transactions per pair is random, but should on average over all pairs approach 'avg_n_trx_per_pair'

    Args:
        n_trx (int): The total number of required transactions

        avg_n_trx_per_pair (int): The average number of transactions per account_id - counterparty_id pair

    Returns:
        pl.DataFrame: Polars Dataframe  containing random recurring transactions between account_id -- counterparty_id pairs
    """

    # Determine the number of  account_id -- counterparty_id pairs to aim for
    number_of_pairs = max(floor(sqrt(n_trx / avg_n_trx_per_pair)), 1)

    # Create random sets to draw from (with replacements)
    possible_acct_ids = [str(uuid.uuid4()) for i in range(number_of_pairs)]
    possible_ctpty_acct_id_ibans = [str(uuid.uuid4()) for i in range(number_of_pairs)]
    possible_dates = [datetime.date(2024, 1, 1) + timedelta(days=i) for i in range(0, 2 * 365)]

    # draw random transaction samples
    data = {
        "date": [random.choice(possible_dates) for i in range(n_trx)],
        "account_id": [random.choice(possible_acct_ids) for i in range(n_trx)],
        "counterparty_id": [random.choice(possible_ctpty_acct_id_ibans) for i in range(n_trx)],
        "booking_amount": [random.randint(1, 100) for i in range(n_trx)],
    }

    # Create Polars DataFrame
    df = pl.DataFrame(data)
    return df
