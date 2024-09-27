# polars-examples

## polars (Python)

### Dependencies

- poetry

### Usage

```shell
poetry install

sh process_gharchive.sh
```

## nodejs-polars

The Node.js part of the project analyzes GitHub activity data using the Polars library in Node.js and visualizes the results using Chart.js. 
The application processes a pre-built JSON file from GitHub Archive, filters relevant events (such as pull requests and issues), and calculates key metrics like the most active contributors, rolling mean of pull requests over time, and the number of open issues over time. 
The data is grouped, aggregated, and then visualized using server-side chart generation with Chart.js. 
The project is modularized into separate components for data loading, metric calculation, and chart rendering.

### Usage
```shell
cd node-metrics

npm install

node src/index.js
```
