import polars as pl
from datetime import datetime, timedelta

df = pl.read_ndjson("gharchive_data_sample.json")

# Filter the DataFrame for 'PullRequestEvent' type and select 'created_at' and nested 'state'
df = (
    df.filter(pl.col("type") == "PullRequestEvent")
    .with_columns(
        pl.col("payload")
        .struct.field("pull_request")
        .struct.field("state")
        .alias("state")
    )
    .select([pl.col("created_at"), pl.col("state")])
)

# Convert 'created_at' to datetime and extract year and month
df = df.with_columns(
    pl.col("created_at")
    .str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%SZ")
    .alias("created_at_dt")
).with_columns(
    pl.col("created_at_dt").dt.year().alias("year"),
    pl.col("created_at_dt").dt.month().alias("month"),
)

# Group by year and month to count the number of open pull requests
open_prs_by_year_month = df.group_by(["year", "month"]).agg(
    pl.count().alias("open_pull_requests")
)

# Display the result
print(open_prs_by_year_month)
