import pandas as pd


def split_based_on_amount(df: pd.DataFrame):
    # calculate median for each group
    df["median_amt_per_subgroup"] = df.groupby(["account_id", "counterparty_id"])["booking_amount"].transform("median")
    # asssign each transsaction to a sequence by comparing the amount to the median amount per group
    df["sequence_id"] = (df["booking_amount"] > df["median_amt_per_subgroup"] * 0.3).astype(int)
    # make sure sequence_ids always start at 0
    df["sequence_id"] = (
        df.groupby(["account_id", "counterparty_id"])["sequence_id"].rank(method="dense", ascending=True) - 1
    )
    return df


def detect_monthly_periodicity(df_transactions: pd.DataFrame) -> pd.DataFrame:
    # order transactions within each sequence by date
    # calculate the days between each transaction
    # assign a monthly periodicity to an interval if the interval is between 25-35 days
    # determine for each sequence the fraction of monthly time intervals

    df_transactions["date"] = pd.to_datetime(df_transactions["date"])
    df_transactions["datediff"] = (
        df_transactions.groupby(["account_id", "counterparty_id", "sequence_id"])["date"].diff().dt.days
    )
    df_transactions["monthly"] = df_transactions["datediff"].between(25, 35)
    df_transactions["monthly_consistency_score"] = df_transactions.groupby(
        ["account_id", "counterparty_id", "sequence_id"]
    )["monthly"].transform(lambda x: x.sum() / (x.count() - 1))
    return df_transactions
