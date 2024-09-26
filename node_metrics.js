import pl from 'nodejs-polars';
import fs from 'fs'; // File system to read JSON files
import { Chart, registerables } from 'chart.js';
import { createCanvas } from 'canvas';
import open from 'open';

// Read File and prepare DataFrame
const filePath = 'node.json';
var df = pl.readJSON(filePath, {format: "lines", inferSchemaLength: 0});

// Compute metrics

// Pre-processing the DataFrame
const createdAtColumnName = "_created_at";
df = df.rename({"id": "_id", "created_at": createdAtColumnName});
df = df.unnest('actor');

// 1. Most active contributors (descending order)
const activeContributors = df.groupBy(['login'])
    .len()
    .sort('login_count', true)
    .dropNulls()
    .filter(pl.col('login').neq(pl.lit('nodejs-github-bot')));
console.log(`Active Contributors: ${activeContributors}`);


// 2. Rolling mean of pull requests over time
var prDf = df.filter(pl.col('type').eq(pl.lit('PullRequestEvent')))
            .withColumn(
                pl.col(createdAtColumnName).str.strptime(pl.Date, '%Y-%m-%dT%H:%M:%S').alias('day')
            );
// Group by day and count pull requests per day
let groupedDf = prDf.groupBy('day').agg(
    pl.count('_id').alias('pr_count')  // Count the number of pull requests per day using the 'id' column
);
// Sort by day to ensure the rolling mean is computed in chronological order
groupedDf = groupedDf.sort('day');
// Calculate rolling mean of the daily pull requests count
groupedDf = groupedDf.withColumn(
    pl.col('pr_count').rollingMean(3).alias('rolling_mean_pr')  // 3-day rolling mean
);
// Select the relevant columns for charting
const rollingMeanPRs = groupedDf.select([
    'day',  // Grouped date (day)
    'rolling_mean_pr'  // Rolling mean of pull requests
]);
console.log(`Rolling Mean PRs: ${rollingMeanPRs}`);


// 3. Number of open issues over time
var issuesDf = df.filter(pl.col('type').eq(pl.lit('IssuesEvent')))
                .unnest('payload')
                .rename({"number": "payload_number"});
// Create a new column 'issue_change' to represent +1 for "opened" and -1 for "closed"
issuesDf = issuesDf.withColumn(
    pl.when(pl.col('action').eq(pl.lit('opened')))
        .then(pl.lit(1))
        .when(pl.col('action').eq(pl.lit('closed')))
        .then(pl.lit(-1))
        .otherwise(pl.lit(0))
        .alias('issue_change')
);
// Group by date or created_at to track open issues over time
issuesDf = issuesDf.sort('_created_at');
// Cumulative sum to track the number of open issues over time
let openIssuesOverTime = issuesDf.select([
    pl.col('_created_at'),
    pl.col('issue_change').cumSum().alias('open_issues_count')  // cumulative sum of open/closed events
]);
console.log(`Open Issues Over Time ${openIssuesOverTime}`);

// DISPLAY //

Chart.register(...registerables); // Register Chart.js components

// Helper function to create and save a chart as an image
async function createChart(config, outputPath) {
  const width = 800;
  const height = 600;
  const canvas = createCanvas(width, height);
  const ctx = canvas.getContext('2d');

  new Chart(ctx, config);

  const buffer = canvas.toBuffer('image/png');
  fs.writeFileSync(outputPath, buffer);
}

// Example: Most Active Contributors Bar Chart
const n = 10; // Only take the top 10 contributors
async function createActiveContributorsChart() {
  const labels = activeContributors.getColumn('login').toArray().slice(0, n);
  const data = activeContributors.getColumn('login_count').toArray().slice(0, n);

  const configuration = {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [{
        label: 'Contributions Count',
        data: data,
        backgroundColor: 'rgba(75, 192, 192, 0.2)',
        borderColor: 'rgba(75, 192, 192, 1)',
        borderWidth: 1
      }]
    },
    options: {
      scales: {
        y: { beginAtZero: true }
      }
    }
  };

  await createChart(configuration, 'active_contributors.png');
}
// Generate the Active Contributors chart
createActiveContributorsChart();


async function createRollingMeanPRsChart() {
    // Extract data for Chart.js
    const labels = rollingMeanPRs.getColumn('day').toArray();  // Dates (days)
    const data = rollingMeanPRs.getColumn('rolling_mean_pr').toArray();  // Rolling mean of pull requests
    // Chart.js configuration
    const configuration = {
        type: 'line',
        data: {
            labels: labels,  // X-axis: day (dates)
            datasets: [{
            label: '7-Day Rolling Mean of Pull Requests',
            data: data,  // Y-axis: rolling mean of pull requests
            backgroundColor: 'rgba(75, 192, 192, 0.2)',
            borderColor: 'rgba(75, 192, 192, 1)',
            borderWidth: 1,
            fill: true,
            tension: 0.4  // Smoothing line
            }]
        },
        options: {
            scales: {
            y: {
                beginAtZero: true,
                title: { display: true, text: 'Rolling Mean of Pull Requests' }
            },
            x: {
                title: { display: true, text: 'Day' }
            }
            }
        }
    };
    await createChart(configuration, 'rolling_mean_prs.png');
}
// Generate the Rolling Mean of PRs chart
createRollingMeanPRsChart();


async function createOpenIssuesOverTimeChart() {
    // Extract labels (timestamps) and data (open issues count)
    const labels = openIssuesOverTime.getColumn('_created_at').toArray();  // Timestamps
    const data = openIssuesOverTime.getColumn('open_issues_count').toArray();  // Open issues count
    const configuration = {
        type: 'line',  // Line chart to show trend over time
        data: {
          labels: labels,  // X-axis: timestamps
          datasets: [{
            label: 'Open Issues Over Time',
            data: data,  // Y-axis: open issues count
            backgroundColor: 'rgba(75, 192, 192, 0.2)',
            borderColor: 'rgba(75, 192, 192, 1)',
            borderWidth: 1,
            fill: true,  // Optional: fill under the line
            tension: 0.4  // Optional: curve the line slightly
          }]
        },
        options: {
          scales: {
            y: {
              beginAtZero: true,  // Start Y-axis from 0
              title: {
                display: true,
                text: 'Number of Open Issues'
              }
            },
            x: {
              title: {
                display: true,
                text: 'Time'
              }
            }
          }
        }
    };
    await createChart(configuration, 'open_issues_over_time.png');
}
// Generate the Open Issues Over Time chart
createOpenIssuesOverTimeChart();

// Open the image with the default image viewer application 
await open('active_contributors.png');
console.log('Exit');