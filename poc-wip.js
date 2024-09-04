const pl = require("nodejs-polars");
const fs = require('fs');
const split2 = require('split2');

let df2 = pl.readJSON('gharchive_data_sample.jsonl', { format: "lines" })
process_df(df2)

// 0. Loading and pre-processing the data
// const readStream = fs.createReadStream('gharchive_data_sample.jsonl', { encoding: 'utf8' });
// const jsonObjects = [];
// readStream.pipe(split2(JSON.parse))
//     .on('data', (jsonObj) => {
//         try {
//             // console.log(jsonObj)
//             jsonObjects.push(jsonObj); // Collect all JSON objects
//         } catch (err) {
//             console.error("Error parsing JSON line:", err);
//         }
//     })
//     .on('end', () => {
//         if (jsonObjects.length > 0) {
//             try {
//                 // Create a DataFrame from an array of JSON objects
//                 const df = pl.DataFrame(jsonObjects);
//                 console.log("2222222222")
//                 console.log(jsonObjects)
//                 console.log("3333333333")
//                 console.log(df)
//                 // process_df(df)
//                 console.log(df);
//             } catch (err) {
//                 console.error("Error creating DataFrame:", err);
//             }
//         } else {
//             console.log("No data was parsed.");
//         }
//     })
//     .on('error', (err) => {
//         console.error("Error reading stream:", err);
//     });



function process_df(df) {

    console.log("Original DF");
    console.log(df);

    // 1. Filtering
    const filteredDf = df.filter(pl.col("type").eq(pl.lit("PullRequestEvent")));


    // 2. Preprocess JSON to Flatten Nested Structure
    console.log("filteredDf");
    console.log(filteredDf);

    const flattenedData = filteredDf.toRecords().map(record => ({
        state: record.payload.pull_request.state,
        merged: record.payload.pull_request.merged,
        user_login: record.payload.pull_request.user.login,
        id: record.id,
        type: record.type,
        actor: record.actor,
        repo: record.repo,
        payload: record.payload,
        public: record.public,
        created_at: record.created_at,
        org: record.org
    }));
    console.log("flattenedData");
    console.log(flattenedData);

    // 2. Create a new DataFrame with Flattened Data
    const nestedDf = pl.DataFrame(flattenedData);
    console.log("nestedDf");
    console.log(nestedDf);

    // 3. Converting string to datetime and extracting parts of the date
    // const dateDf = nestedDf.withColumns([
    //     pl
    //         .col("created_at")
    //         .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ")
    //         .alias("created_at_dt"),
    //     pl
    //         .col("created_at")
    //         .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ")
    //         .dt.year()
    //         .alias("year"),
    //     pl
    //         .col("created_at")
    //         .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ")
    //         .dt.month()
    //         .alias("month"),
    // ]);

    // 4. Aggregations and grouping

    // 5. Sorting the DataFrame

    // 6. Joining two DataFrames

    // 7. Calculate monthly pull request counts

    // 8. Calculate moving average (3-month window)

    // 9. Rank pull requests within each year

    // Print the results
    //     console.log("Filtered DataFrame:");
    //     console.log(filteredDf);

    //     console.log("\nNested DataFrame with renamed columns:");
    //     console.log(nestedDf);

    //     console.log("\nDate DataFrame with year and month extracted:");
    //     console.log(dateDf);

    //     console.log("\nAggregated DataFrame by state:");
    //     console.log(aggregatedDf);

    //     console.log("\nSorted DataFrame by created_at_dt:");
    //     console.log(sortedDf);

    //     console.log("\nJoined DataFrame with user details:");
    //     console.log(joinedDf);

    //     console.log("\nMonthly Counts and Moving Average:");
    //     console.log(monthlyCountsWithMovingAvg);

    //     console.log("\nRanked Pull Requests within Each Year:");
    //     console.log(rankedDf);
}