# Analyzing GitHub Events Data with Polars: A Step-by-## Step Guide

If you've ever wondered how to efficiently process large-scale JSON data, this blog post will guide you through using Polars, a lightning-fast DataFrame library for Python. We'll take you through a real-world example using GitHub event data, performing various data manipulation tasks that are common in data analysis workflows.

All snnipets in this article can be found in this [repository](https://github.com/nearform/polars-examples)

## Step 1: Reading the Data

We start by reading a JSON file containing GitHub events data. For this example, we are using a sample NDJSON (Newline Delimited JSON) file. Polars makes this straightforward with the read_ndjson function.

```python
import polars as pl

df = pl.read_ndjson("gharchive_data_sample.json")
```

## Step 2: Filtering Data

Suppose we are interested only in PullRequestEvent types. We can filter the DataFrame accordingly:

```python
filtered_df = df.filter(pl.col("type") == "PullRequestEvent")
```

## Step 3: Accessing Nested Fields and Renaming Columns

GitHub's event data is nested, which can make it tricky to access relevant fields. Polars allows us to drill down into nested structures and rename columns for clarity.

```python
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
```

## Step 4: Working with Dates

Date and time manipulation is a common task in data analysis. Here, we convert string representations of dates into actual datetime objects and extract the year and month.

```python
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
```

## Step 5: Aggregations and Grouping

Aggregating data helps summarize and understand the distribution of events. Here, we group by the state of the pull request and count occurrences, also summing up the merged ones.

```python
aggregated_df = date_df.group_by("state").agg(
    [pl.count("id").alias("count"), pl.col("merged").sum().alias("merged_count")]
)
```

## Step 6: Sorting the DataFrame

Sorting data by specific columns can provide valuable insights. Here, we sort by the datetime column in descending order.

```python
sorted_df = date_df.sort("created_at_dt", descending=True)
```

## Step 7: Joining DataFrames

Data often needs to be combined from multiple sources. We join our date DataFrame with a user information DataFrame based on login details.

```python
users_data = [
    {"login": "mevlan", "full_name": "User One"},
    {"login": "adneon0", "full_name": "User Two"},
    {"login": "codeschool-kiddo", "full_name": "User Three"},
]
users_df = pl.DataFrame(users_data)

joined_df = date_df.join(users_df, left_on="user_login", right_on="login", how="inner")
```

## Step 8: Monthly Pull Request Counts and Moving Average

Analyzing trends over time, such as monthly pull request counts, can reveal patterns. We use group-by operations and compute a rolling mean to smooth out fluctuations.

```python
monthly_counts = date_df.group_by(["year", "month"]).agg(
    [pl.count().alias("monthly_count")]
)

monthly_counts = monthly_counts.with_columns(
    [
        pl.col("monthly_count")
        .rolling_mean(window_size=3, min_periods=1)
        .alias("moving_avg")
    ]
)
```

## Step 9: Ranking Events

Ranking events within groups can be useful for prioritizing or identifying trends. Here, we rank pull requests within each year.

```python
ranked_df = date_df.with_columns(
    [pl.col("created_at_dt").sort().cum_count().over("year").alias("rank_within_year")]
)
```

## Conclusion

Polars provides a powerful and intuitive way to handle large-scale data manipulation tasks efficiently. From filtering and accessing nested fields to complex aggregations and joins, Polars' syntax is both expressive and easy to understand. This makes it an excellent choice for data scientists and engineers who need to process large datasets rapidly.

By following these steps, you can replicate similar data analysis workflows and gain insights from large and complex datasets.