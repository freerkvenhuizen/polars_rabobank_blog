import polars as pl


def split_based_on_amount(df_transactions: pl.LazyFrame) -> pl.LazyFrame:

    # calculate median for each group
    # asssign each transsaction to a sequence by comparing the amount to the median amount per group
    # make sure sequence_ids always start at 0
    df_split_based_on_amount = df_transactions.with_columns(
        sequence_id=(
            (pl.col("booking_amount") > (pl.median("booking_amount") * 2)).rank("dense", descending=False)
        ).over("account_id", "counterparty_id")
        - 1,
    )

    return df_split_based_on_amount


def detect_monthly_periodicity(df_transactions: pl.LazyFrame) -> pl.LazyFrame:
    # order transactions within each sequence by date
    # calculate the days between each transaction
    # assign a monthly periodicity to an interval if the interval is between 25-35 days
    # determine for each sequence the fraction of monthly time intervals

    df_res = df_transactions.with_columns(
        monthly=pl.col("date")
        .dt.date()
        .diff(null_behavior="ignore")
        .dt.total_days()
        .is_between(25, 35)
        .over("account_id", "counterparty_id", "sequence_id", order_by="date")
    ).with_columns(monthly_consistency_score=pl.mean("monthly").over("account_id", "counterparty_id", "sequence_id"))
    return df_res
