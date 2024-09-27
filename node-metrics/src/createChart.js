import { createCanvas } from 'canvas';
import { Chart, registerables } from 'chart.js';
import fs from 'fs'; // File system to read JSON files

Chart.register(...registerables); // Register Chart.js components

// Function to create a chart (example for one chart type)
export const createChart = async (config, filePath) => {
    const width = 1200;
    const height = 900;
    const canvas = createCanvas(width, height);
    const ctx = canvas.getContext('2d');

    new Chart(ctx, config);

    // Generate the chart as an image and save it
    const buffer = canvas.toBuffer('image/png');
    fs.writeFileSync(filePath, buffer);
};
