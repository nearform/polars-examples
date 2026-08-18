# polars-examples

## Overview

This repository contains a set of Shell and Python scripts for downloading, unzipping, cleaning, and filtering GitHub event data specifically for the nodejs/node repository. The data is then processed and analyzed using Polars in Node.js to generate key metrics and visualizations.

### Dependencies

**Poetry >= 2.0** for managing Python dependencies. `poetry.lock` is `lock-version = "2.1"`,
written by Poetry 2.1. Poetry 1.x still reads that lockfile, but only with a compatibility
warning ("The lock file might not be compatible with the current version of Poetry"), and it
may not interpret it correctly, so Poetry >= 2.0 is the supported floor.
`requires-poetry = ">=2.0"` in `pyproject.toml` makes that explicit for anyone on 2.x, which
reports "This project requires Poetry >=2.0". Poetry 1.8.3 does not recognise the key and
stops with a less helpful message: "Additional properties are not allowed
('requires-poetry' was unexpected)".

**Node.js `^18.12.0 || >= 20.9.0`** for the `node-metrics` analysis step. That is the range
`canvas@3` requires and the narrowest floor in `node-metrics`; it is declared in
`node-metrics/package.json`.

#### Setup and Usage

##### 1. Download, Clean, and Filter Data

First, install the required dependencies and execute the script to download and preprocess the data:

```shell
poetry install
sh process_gharchive.sh
```

This will generate a cleaned JSON file at `data/final/node.json`. This file should then be copied to the `node-metrics` directory to be used for further analysis.

```shell
cp data/final/node.json node-metrics/data/node.json
```

##### 2. Analyze Data with Node.js

The Node.js part of the project processes and visualizes the pre-built JSON data using the Polars library and Chart.js. It calculates key metrics such as the most active contributors, the rolling mean of pull requests over time, and the number of open issues.

To set up and run the analysis:

```shell
cd node-metrics
npm install
node src/index.js
```

This will generate server-side charts to visualize the metrics.

### Project Structure

#### Shell/Python Scripts

Scripts for data download, extraction, cleaning, and filtering.
Outputs a final JSON file with the processed nodejs/node events.

#### Node.js Scripts

Analyzes the pre-processed JSON data.
Uses Polars for data manipulation and Chart.js for visualization.
Includes modular components for data loading, metric calculation, and chart rendering.

#### Notes

Ensure you have all dependencies installed before running the scripts.
