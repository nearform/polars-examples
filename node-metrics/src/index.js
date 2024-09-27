import { loadData } from './dataLoader.js';
import { calculateActiveContributors, calculateRollingMeanPR, calculateOpenIssuesOverTime } from './metricsCalculator.js';
import { createActiveContributorsChart, createRollingMeanPRsChart, createOpenIssuesOverTimeChart } from './visualizer.js';
import open from 'open';

// Main function to run the analysis and visualization
(async () => {
    const filePath = './data/node.json';
    const df = loadData(filePath);

    // Metrics calculation
    const activeContributors = calculateActiveContributors(df);
    const rollingMeanPRs = calculateRollingMeanPR(df, '_created_at', 3);
    const openIssuesOverTime = calculateOpenIssuesOverTime(df, '_created_at');

    // Visualization

    const n = 10; // Only take the top 10 contributors
    const activeContributorsFilePath = './charts/active_contributors.png';
    await createActiveContributorsChart(
        activeContributors.getColumn('login').toArray().slice(0, n),
        activeContributors.getColumn('login_count').toArray().slice(0, n),
        activeContributorsFilePath
    );

    await createRollingMeanPRsChart(
        rollingMeanPRs.getColumn('day').toArray(),
        rollingMeanPRs.getColumn('rolling_mean_pr').toArray(),
        './charts/rolling_mean_prs.png'
    );

    await createOpenIssuesOverTimeChart(
        openIssuesOverTime.getColumn('_created_at').toArray(),
        openIssuesOverTime.getColumn('open_issues_count').toArray(),
        './charts/open_issues_over_time.png'
    );

    // Open the image with the default image viewer application 
    await open(activeContributorsFilePath);
    console.log('Exit');
})();
