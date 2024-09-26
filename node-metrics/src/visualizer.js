import {createChart} from './createChart.js'

export const createActiveContributorsChart = async (labels, data, filePath) => {
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
  
    await createChart(configuration, filePath);
};

export const createRollingMeanPRsChart = async (labels, data, filePath) => {
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
    await createChart(configuration, filePath);
};

export const createOpenIssuesOverTimeChart = async (labels, data, filePath) => {
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
    await createChart(configuration, filePath);
}
