import polars as pl

df = pl.DataFrame()

df_monthly_sequences = df.with_columns(
    monthly=pl.col("date")
    .dt.date()
    .diff(null_behavior="ignore")
    .dt.total_days()
    .is_between(25, 35)
    .over("account", "counterparty", "sequence_id", order_by="date")
).with_columns(monthly_consistency_score=pl.mean("monthly").over("account_id", "counterparty_id", "sequence_id"))
