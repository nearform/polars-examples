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
)

# Get the first day of this month
today = datetime.today()
first_day_of_this_month = datetime(today.year, today.month, 1)

# Get the first day of last month
last_month_start = first_day_of_this_month - timedelta(days=1)
last_month_start = datetime(last_month_start.year, last_month_start.month, 1)

# Get the first day of this month
this_month_start = datetime(today.year, today.month, 1)

# Filter to get only the pull requests opened last month
open_prs_last_month = df.filter(
    (pl.col("created_at_dt") >= last_month_start)
    & (pl.col("created_at_dt") < this_month_start)
).shape[0]

# Display the number of open pull requests opened last month
print(f"Number of open pull requests opened last month: {open_prs_last_month}")
