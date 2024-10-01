import {createChart} from './createChart.js'
import 'chartjs-adapter-date-fns';  // Import the date adapter

export const createActiveContributorsChart = async (labels, data, filePath) => {
    const configuration = {
      type: 'bar',
      data: {
        labels: labels,
        datasets: [{
          label: 'Contributions Count',
          data: data,
          backgroundColor: 'rgba(31, 87, 159, 0.2)', // blue-ish color
          borderColor: 'rgba(31, 87, 159, 1)',
          borderWidth: 1
        }]
      },
      options: {
        scales: {
            x: {
                ticks: {
                    // Rotate labels if necessary
                    maxRotation: 45,
                    minRotation: 0,
                    font: {
                        size: 14  // Increase font size
                    }
                },
                title: {
                    display: true,
                    text: 'Contributors',
                    font: {
                        size: 14  // Increase font size for x-axis title
                    }
                }
            },
            y: {
                beginAtZero: true,
                ticks: {
                    font: {
                        size: 14  // Increase font size for Y-axis
                    }
                }
            }
        },
        plugins: {
            legend: {
                labels: {
                    font: {
                        size: 16  // Increase font size for legend
                    }
                }
            },
            title: {
                display: true,
                text: 'Most Active Contributors',
                font: {
                    size: 18  // Increase title font size
                }
            }
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
            label: '3-Day Rolling Mean of Pull Requests',
            data: data,  // Y-axis: rolling mean of pull requests
            backgroundColor: 'rgba(27, 105, 53, 0.2)', // green-ish color
            borderColor: 'rgba(27, 105, 53, 1)',
            borderWidth: 1,
            fill: true,
            tension: 0.4  // Smoothing line
            }]
        },
        options: {
            scales: {
                y: {
                    beginAtZero: true,
                    title: { display: false, text: 'Rolling Mean of Pull Requests' }
                },
                x: {
                    title: { display: false, text: 'Date' },
                    type: 'time',
                    time: {
                        unit: 'day',
                        displayFormats: {
                          day: 'yyyy-MM-dd'  // Display format for x-axis
                      }
                    }
                }
            },
            plugins: {
              legend: {
                  labels: {
                      font: {
                          size: 16  // Increase font size for legend
                      }
                  }
              },
              title: {
                  display: true,
                  text: 'Rolling Mean of Pull Requests',
                  font: {
                      size: 18  // Increase title font size
                  }
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
            backgroundColor: 'rgba(129, 36, 36, 0.2)', // red-ish color
            borderColor: 'rgba(129, 36, 36, 1)',
            borderWidth: 1,
            fill: true,  // Optional: fill under the line
            tension: 0.4  // Optional: curve the line slightly
          }]
        },
        options: {
          scales: {
              x: {
                  type: 'time',  // Use time scale for the x-axis
                  time: {
                      unit: 'day',  // Group data by day
                      displayFormats: {
                          day: 'yyyy-MM-dd'  // Display format for x-axis
                      }
                  },
                  ticks: {
                      font: {
                          size: 12  // Increase font size for x-axis labels
                      }
                  },
                  title: {
                      display: false,
                      text: 'Date',
                      font: {
                          size: 14  // Increase font size for x-axis title
                      }
                  }
              },
              y: {
                  beginAtZero: true,
                  ticks: {
                      font: {
                          size: 14  // Increase font size for y-axis labels
                      }
                  },
              }
          },
          plugins: {
              legend: {
                  labels: {
                      font: {
                          size: 16  // Increase font size for legend
                      }
                  }
              },
              title: {
                  display: true,
                  text: 'Open Issues Over Time',
                  font: {
                      size: 18  // Increase title font size
                  }
              },
              tooltip: {
                  titleFont: { size: 14 },  // Increase font size for tooltip titles
                  bodyFont: { size: 12 }    // Increase font size for tooltip body
              }
          }
      }
    };
    await createChart(configuration, filePath);
}
