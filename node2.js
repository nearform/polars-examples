import pl from 'nodejs-polars';
import fs from 'fs'; // File system to read JSON files
import { Chart, registerables } from 'chart.js';
import { createCanvas } from 'canvas';
import open, {openApp, apps} from 'open';

const df = pl.readJSON('node_input/2024-01-01-12.copy.json', {format: "lines"});

// Compute metrics

// 1. Most active contributors
const activeContributors = df.groupBy(['login']).count().sort('login_count', false).dropNulls();
console.log("active Contributors");
console.log(activeContributors);

// 2. Rolling mean of pull requests over time (assuming 'created_at' field exists)
var prDf = df.filter(pl.col('type').eq(pl.lit('PullRequestEvent')));
prDf = prDf.withColumns([pl.col('created_at').str.strptime(pl.Date, '%Y-%m-%dT%H:%M:%S')]).sort('created_at');
//console.log(prDf);

const rollingMeanPRs = prDf.groupByRolling(
    {indexColumn: 'created_at', period: '30d'}
).agg(
    pl.col('id').count().alias('rolling_mean_prs')
);
console.log("rolling Mean PRs");
console.log(rollingMeanPRs);

// 3. Number of open issues over time (assuming 'issue' and 'state' fields exist)
var issueDf = df.rename({"id": "_id", "created_at": "_created_at"}).filter(pl.col('type').eq(pl.lit('IssuesEvent')));
//console.log(issueDf);
issueDf = issueDf.unnest('payload').rename({"number": "_number"});
//console.log(issueDf);
issueDf = issueDf.unnest('issue');
//console.log(issueDf);
console.log("open Issues Over Time");
const openIssuesOverTime = issueDf.filter(pl.col('state').eq(pl.lit('open')))
  .groupBy('created_at').count();

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
console.log(activeContributors.getColumn('login').toArray());
async function createActiveContributorsChart() {
  const labels = activeContributors.getColumn('login').toArray();
  const data = activeContributors.getColumn('login_count').toArray();

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

// Open the image with the default image viewer application 
await open('active_contributors.png');
console.log('Exit');