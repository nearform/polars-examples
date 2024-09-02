const pl = require("nodejs-polars");
const fs = require('fs');
const split2 = require('split2');

// 0. Loading and pre-processing the data
const readStream = fs.createReadStream('gharchive_data_sample.jsonl', { encoding: 'utf8' });
const jsonObjects = [];
readStream.pipe(split2(JSON.parse))
    .on('data', (jsonObj) => {
        try {
            jsonObjects.push(jsonObj); // Collect all JSON objects
        } catch (err) {
            console.error("Error parsing JSON line:", err);
        }
    })
    .on('end', () => {
        if (jsonObjects.length > 0) {
            try {
                // Create a DataFrame from an array of JSON objects
                const df = pl.DataFrame(jsonObjects);
                process_df(df)
                console.log(df);
            } catch (err) {
                console.error("Error creating DataFrame:", err);
            }
        } else {
            console.log("No data was parsed.");
        }
    })
    .on('error', (err) => {
        console.error("Error reading stream:", err);
    });



function process_df(df) {


    // 1. Filtering
    const filteredDf = df.filter(pl.col("type").eq(pl.lit("PullRequestEvent")));


    // 2. Accessing nested fields and renaming columns

    // 3. Converting string to datetime and extracting parts of the date

    // 4. Aggregations and grouping

    // 5. Sorting the DataFrame

    // 6. Joining two DataFrames

    // 7. Calculate monthly pull request counts

    // 8. Calculate moving average (3-month window)

    // 9. Rank pull requests within each year

    // Print the results
    //     console.log("Filtered DataFrame:");
    //     console.log(filteredDf.toString());

    //     console.log("\nNested DataFrame with renamed columns:");
    //     console.log(nestedDf.toString());

    //     console.log("\nDate DataFrame with year and month extracted:");
    //     console.log(dateDf.toString());

    //     console.log("\nAggregated DataFrame by state:");
    //     console.log(aggregatedDf.toString());

    //     console.log("\nSorted DataFrame by created_at_dt:");
    //     console.log(sortedDf.toString());

    //     console.log("\nJoined DataFrame with user details:");
    //     console.log(joinedDf.toString());

    //     console.log("\nMonthly Counts and Moving Average:");
    //     console.log(monthlyCountsWithMovingAvg.toString());

    //     console.log("\nRanked Pull Requests within Each Year:");
    //     console.log(rankedDf.toString());
}