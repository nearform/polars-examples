import pl from 'nodejs-polars';

// Calculate most active contributors
export const calculateActiveContributors = (df) => {
    return df
    // Rename columns to avoid column name duplication during unnesting 'actor' field
    .rename({ "id": "_id", "created_at": "_created_at"})
    .unnest('actor')
    .groupBy(['login'])
        .len()
        .sort('login_count', true)
        .dropNulls()
        .filter(pl.col('login').neq(pl.lit('nodejs-github-bot'))); // filter out 'nodejs-github-bot' user
};

// Calculate rolling mean of pull requests over time
export const calculateRollingMeanPR = (df, rollingMeanDays) => {
    const prDf = df.filter(pl.col('type').eq(pl.lit('PullRequestEvent')))
        .withColumn(pl.col("created_at").str.strptime(pl.Date, '%Y-%m-%dT%H:%M:%S').alias('day'));

    // Group by day and count pull requests per day
    let groupedDf = prDf.groupBy('day').agg(pl.count('id').alias('pr_count'));

    groupedDf = groupedDf.sort('day');
    
    // Calculate rolling mean of the daily pull requests count
    return groupedDf.withColumn(pl.col('pr_count').rollingMean(rollingMeanDays).alias('rolling_mean_pr'))
        .select(['day', 'rolling_mean_pr']);
};

// Calculate the number of open issues over time
export const calculateOpenIssuesOverTime = (df) => {
    let issuesDf = df.filter(pl.col('type').eq(pl.lit('IssuesEvent')))
        .unnest('payload');

    // Create a new column 'issue_change' to represent +1 for "opened" and -1 for "closed"
    issuesDf = issuesDf.withColumn(
        pl.when(pl.col('action').eq(pl.lit('opened')))
            .then(pl.lit(1))
            .when(pl.col('action').eq(pl.lit('closed')))
            .then(pl.lit(-1))
            .otherwise(pl.lit(0))
            .alias('issue_change')
    );

    issuesDf = issuesDf.sort("created_at");

    // Cumulative sum to track the number of open issues over time
    return issuesDf.select([
        pl.col("created_at"),
        pl.col('issue_change').cumSum().alias('open_issues_count')
    ]);
};
