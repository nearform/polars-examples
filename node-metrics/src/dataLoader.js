import pl from 'nodejs-polars';

export const loadData = (filePath) => {
    console.log(`Reading input file ${filePath}`);

    const eventsDataFrame = pl.readJSON(filePath, { format: "lines", inferSchemaLength: 0 });
    return eventsDataFrame;
};
