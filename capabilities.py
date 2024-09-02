import polars as pl

df = pl.read_ndjson("gharchive_data_sample.json")

# 1. Filtering
filtered_df = df.filter(pl.col("type") == "PullRequestEvent")

# 2. Accessing nested fields and renaming columns
nested_df = filtered_df.with_columns(
    [
        pl.col("payload")
        .struct.field("pull_request")
        .struct.field("state")
        .alias("state"),
        pl.col("payload")
        .struct.field("pull_request")
        .struct.field("merged")
        .alias("merged"),
        pl.col("payload")
        .struct.field("pull_request")
        .struct.field("user")
        .struct.field("login")
        .alias("user_login"),
    ]
)

# 3. Converting string to datetime and extracting parts of the date
date_df = nested_df.with_columns(
    [
        pl.col("created_at")
        .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ")
        .alias("created_at_dt"),
        pl.col("created_at")
        .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ")
        .dt.year()
        .alias("year"),
        pl.col("created_at")
        .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ")
        .dt.month()
        .alias("month"),
    ]
)

# 4. Aggregations and grouping
aggregated_df = date_df.group_by("state").agg(
    [pl.count("id").alias("count"), pl.col("merged").sum().alias("merged_count")]
)

# 5. Sorting the DataFrame
sorted_df = date_df.sort("created_at_dt", descending=True)

# 6. Joining two DataFrames
users_data = [
    {"login": "mevlan", "full_name": "User One"},
    {"login": "adneon0", "full_name": "User Two"},
    {"login": "codeschool-kiddo", "full_name": "User Three"},
]
users_df = pl.DataFrame(users_data)

# 7. Join date_df with users_df on 'user_login' and 'login'
joined_df = date_df.join(users_df, left_on="user_login", right_on="login", how="inner")

# 8. Calculate monthly pull request counts
monthly_counts = date_df.group_by(["year", "month"]).agg(
    [pl.count().alias("monthly_count")]
)
# 9. Calculate moving average (3-month window)
monthly_counts = monthly_counts.with_columns(
    [
        pl.col("monthly_count")
        .rolling_mean(window_size=3, min_periods=1)
        .alias("moving_avg")
    ]
)

# 10. Rank pull requests within each year
ranked_df = date_df.with_columns(
    [pl.col("created_at_dt").sort().cum_count().over("year").alias("rank_within_year")]
)

# Print the results
print("Filtered DataFrame:")
print(filtered_df)

print("\nNested DataFrame with renamed columns:")
print(nested_df)

print("\nDate DataFrame with year and month extracted:")
print(date_df)

print("\nAggregated DataFrame by state:")
print(aggregated_df)

print("\nSorted DataFrame by created_at_dt:")
print(sorted_df)

print("\nJoined DataFrame with user details:")
print(joined_df)

print("\nMonthly Counts and Moving Average:")
print(monthly_counts)

print("\nRanked Pull Requests within Each Year:")
print(ranked_df)
