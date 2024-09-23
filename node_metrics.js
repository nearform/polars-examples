const pl = require('nodejs-polars');
const fs = require('fs');
//const { ChartJSNodeCanvas } = require('chartjs-node-canvas');
const { Chart, registerables } = require('chart.js');
const { createCanvas } = require('canvas');

// Load the JSON file
const rawData = fs.readFileSync('node_input/2024-01-01-12.json');
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
async function createActiveContributorsChart() {
  const labels = ['contributor1', 'contributor2', 'contributor3']; // Replace with actual data
  const data = [10, 15, 5]; // Replace with actual data

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

// Generate the chart
createActiveContributorsChart();
