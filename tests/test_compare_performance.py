import pytest
from pytest_benchmark.fixture import BenchmarkFixture

from src.solution_pandas import (
    detect_monthly_periodicity as detect_monthly_periodicity_pandas,
)
from src.solution_pandas import split_based_on_amount as split_based_on_amount_pandas
from src.solution_polars import (
    detect_monthly_periodicity as detect_monthly_periodicity_polars,
)
from src.solution_polars import split_based_on_amount as split_based_on_amount_polars
from src.utils.generate_transactions import generate_random_transactions


@pytest.mark.parametrize("solution", ["polars", "pandas"])
@pytest.mark.parametrize("n_trx", [10, 100, 1000, 10_000, 100_000, 1_000_000])
def test_compare_performance(n_trx: int, benchmark: BenchmarkFixture, solution: str) -> None:

    df_test_data_polars = generate_random_transactions(n_trx, 10)
    df_test_data_pandas = df_test_data_polars.to_pandas()

    if solution == "polars":

        def to_benchmark() -> None:
            df_lazy = df_test_data_polars.lazy()
            df_split = split_based_on_amount_polars(df_lazy)
            df_periodicity_assigned = detect_monthly_periodicity_polars(df_split)
            df_periodicity_assigned.collect(engine="in-memory")

    elif solution == "pandas":

        def to_benchmark() -> None:
            df_split = split_based_on_amount_pandas(df_test_data_pandas)
            _ = detect_monthly_periodicity_pandas(df_split)

    benchmark(to_benchmark)
