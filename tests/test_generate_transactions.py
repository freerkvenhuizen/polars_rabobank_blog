import pytest

from src.utils.generate_transactions import generate_random_transactions


@pytest.mark.parametrize("n_trx", [10, 100, 1000, 10000, 100000])
@pytest.mark.parametrize("n_trx_per_pair", [1, 10, 100])
def test_generate_trx(n_trx: int, n_trx_per_pair: int) -> None:

    df = generate_random_transactions(n_trx, n_trx_per_pair)
    # the resulting DataFrame should have the required number of rows
    assert df.height == n_trx
