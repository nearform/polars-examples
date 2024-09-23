const pl = require('nodejs-polars');
const fs = require('fs');
const { ChartJSNodeCanvas } = require('chartjs-node-canvas');

// Load the JSON file
const rawData = fs.readFileSync('node_input/2024-01-01-13.json');
const events = JSON.parse(rawData);

// Convert JSON into a Polars DataFrame
const df = pl.DataFrame(events);

// Compute metrics
// 1. Most active contributors
const activeContributors = df.groupby('actor.login').count().sort('count', false);

// 2. Rolling mean of pull requests over time (assuming 'created_at' field exists)
const prDf = df.filter(pl.col('type').eq('PullRequestEvent'));
prDf = prDf.withColumn(pl.col('created_at').str.strptime(pl.Date, '%Y-%m-%dT%H:%M:%S'))
  .sort('created_at');
const rollingMeanPRs = prDf.groupbyRolling({
  index_column: 'created_at',
  period: '30d'
}).agg(pl.col('id').count().alias('rolling_mean_prs'));

// 3. Number of open issues over time (assuming 'issue' and 'state' fields exist)
const issueDf = df.filter(pl.col('type').eq('IssuesEvent'));
const openIssuesOverTime = issueDf.filter(pl.col('issue.state').eq('open'))
  .groupby('created_at').count();

// DISPLAY //

// Create a ChartJSNodeCanvas instance for rendering charts
const width = 800; 
const height = 600;
const chartJSNodeCanvas = new ChartJSNodeCanvas({ width, height });

// Most Active Contributors Bar Chart
async function createActiveContributorsChart() {
  const labels = activeContributors.getColumn('actor.login').toArray();
  const data = activeContributors.getColumn('count').toArray();

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

  const image = await chartJSNodeCanvas.renderToBuffer(configuration);
  fs.writeFileSync('active_contributors.png', image);
}

// Rolling Mean of Pull Requests Over Time Line Chart
async function createRollingMeanPRChart() {
  const labels = rollingMeanPRs.getColumn('created_at').toArray();
  const data = rollingMeanPRs.getColumn('rolling_mean_prs').toArray();

  const configuration = {
    type: 'line',
    data: {
      labels: labels,
      datasets: [{
        label: 'Rolling Mean of PRs',
        data: data,
        borderColor: 'rgba(54, 162, 235, 1)',
        tension: 0.1
      }]
    }
  };

  const image = await chartJSNodeCanvas.renderToBuffer(configuration);
  fs.writeFileSync('rolling_mean_prs.png', image);
}

// Number of Open Issues Over Time Line Chart
async function createOpenIssuesChart() {
  const labels = openIssuesOverTime.getColumn('created_at').toArray();
  const data = openIssuesOverTime.getColumn('count').toArray();

  const configuration = {
    type: 'line',
    data: {
      labels: labels,
      datasets: [{
        label: 'Open Issues',
        data: data,
        borderColor: 'rgba(255, 99, 132, 1)',
        tension: 0.1
      }]
    }
  };

  const image = await chartJSNodeCanvas.renderToBuffer(configuration);
  fs.writeFileSync('open_issues.png', image);
}

// Generate Charts
createActiveContributorsChart();
createRollingMeanPRChart();
createOpenIssuesChart();
